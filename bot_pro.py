# bot_pro.py
# FF Calc PRO — партия до 10 SKU
# services.xlsx: наименование | ед.изм | цена | категория(работа/расходники)
# shipping.xlsx: 2 листа (одинаковые колонки: код | наименование | ед.изм | цена)
#
# 1) В выборе склада показываем ТОЛЬКО реальные склады:
#    - Лист FF: только строки с кодом FF_SHIP_BOX_...
#    - Лист TK: только строки, где код начинается с TK_  (остальные считаем константами и игнорим как "склад")
#
# 2) Паллетизация по объёму паллеты:
#    - Максимальный объём паллеты = 16 коробов 60×40×40
#    - Для любого другого размера короба:
#        max_boxes_on_pallet = floor(pallet_volume / box_volume)
#    - pallets = ceil(total_boxes / max_boxes_on_pallet) если total_boxes >= threshold
#    - стретч = 1 на паллету
#
# 3) Менеджеру приходит структурированная заявка:
#    - Услуги по категориям с ценой и ед.изм
#    - Итоги по SKU и по партии

import os
import math
import logging
from typing import Dict, Tuple, List, Optional

import pandas as pd
from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.dispatcher import FSMContext

# =========================
# ENV
# =========================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
MANAGER_CHAT_ID_RAW = os.getenv("MANAGER_CHAT_ID", "").strip()

if not BOT_TOKEN:
    raise RuntimeError("Не задан BOT_TOKEN. В PowerShell: $env:BOT_TOKEN='...token...'")

try:
    MANAGER_CHAT_ID = int(MANAGER_CHAT_ID_RAW)
    if MANAGER_CHAT_ID <= 0:
        raise ValueError
except Exception:
    raise RuntimeError("Не задан корректный MANAGER_CHAT_ID. В PowerShell: $env:MANAGER_CHAT_ID='123456789'")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SERVICES_XLSX = os.path.join(BASE_DIR, "services.xlsx")
SHIPPING_XLSX = os.path.join(BASE_DIR, "shipping.xlsx")

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN, parse_mode=types.ParseMode.HTML)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# =========================
# CONSTANTS
# =========================

MAX_SKU = 10

BOX_PRICE = 110.0

# Короба склада (выбор)
WAREHOUSE_BOXES = {
    "60×40×40": (60.0, 40.0, 40.0),
    "40×30×30": (40.0, 30.0, 30.0),
}

# Базовый объём паллеты = 16 коробов 60×40×40
BASE_PALLET_BOX = (60.0, 40.0, 40.0)
BASE_BOXES_PER_PALLET = 16

# =========================
# XLSX loaders
# =========================

def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df

def _need_cols(df: pd.DataFrame, tag: str) -> pd.DataFrame:
    need = ["код", "наименование", "ед.изм", "цена"]
    for n in need:
        if n not in df.columns:
            raise RuntimeError(f"{tag}: не вижу колонку '{n}'. Колонки: {df.columns.tolist()}")
    df["цена"] = pd.to_numeric(df["цена"], errors="coerce")
    df = df[df["цена"].notna()].copy()
    return df

def load_services_from_xlsx(path: str) -> Dict[str, Tuple[str, float, str, str]]:
    """
    services.xlsx columns:
      наименование | ед.изм | цена | категория
    Возвращает SERVICES:
      key = русское наименование (как на кнопке)
      value = (name, price, category, unit)
    """
    if not os.path.exists(path):
        raise RuntimeError(f"Не найден файл services.xlsx: {path}")

    df = pd.read_excel(path, sheet_name=0)
    df = _norm_cols(df)

    need = ["наименование", "ед.изм", "цена"]
    for n in need:
        if n not in df.columns:
            raise RuntimeError(f"services.xlsx: не вижу колонку '{n}'. Колонки: {df.columns.tolist()}")

    if "категория" not in df.columns:
        df["категория"] = "работа"

    df["цена"] = pd.to_numeric(df["цена"], errors="coerce")
    df = df[df["цена"].notna()].copy()

    services: Dict[str, Tuple[str, float, str, str]] = {}
    for _, r in df.iterrows():
        name = str(r["наименование"]).strip()
        unit = str(r["ед.изм"]).strip()
        price = float(r["цена"])
        cat = str(r["категория"]).strip().lower()

        if not name or name.lower() == "nan":
            continue
        if not unit or unit.lower() == "nan":
            unit = "шт"
        if cat not in ["работа", "расходники"]:
            cat = "работа"

        services[name] = (name, price, cat, unit)

    if not services:
        raise RuntimeError("services.xlsx: после чтения не осталось ни одной услуги (проверь данные).")

    return services

def load_shipping_from_xlsx(path: str) -> Tuple[Dict[str, dict], Dict[str, dict], Dict[str, float]]:
    """
    shipping.xlsx: 2 листа.

    Лист 1 (FF):
      - СКЛАДЫ: только код FF_SHIP_BOX_... => наименование=склад, цена=ставка/короб
      - КОНСТАНТЫ: всё остальное (PALLET, STRETCH, PALLET_THRESHOLD_BOXES, TO_TK_PER_BOX ...),
        префикс FF_ снимаем (FF_PALLET -> PALLET)

    Лист 2 (TK):
      - СКЛАДЫ: только строки с кодом TK_... => наименование=склад (одинаковое для его ставок)
        * если код содержит PALLET/ПАЛЛЕТ -> ставка "до склада за паллету"
        * если код содержит ENTRY/ВЪЕЗД -> разовый въезд
        * иначе -> ставка "до склада за короб"
      - Любые строки без TK_ считаем НЕ складами (константы/служебные) и игнорируем в списке складов.
    """
    if not os.path.exists(path):
        raise RuntimeError(f"Не найден файл shipping.xlsx: {path}")

    ff_df = pd.read_excel(path, sheet_name=0)
    tk_df = pd.read_excel(path, sheet_name=1)

    ff_df = _need_cols(_norm_cols(ff_df), "shipping.xlsx (лист 1 FF)")
    tk_df = _need_cols(_norm_cols(tk_df), "shipping.xlsx (лист 2 TK)")

    FF_WAREHOUSES: Dict[str, dict] = {}
    TK_WAREHOUSES: Dict[str, dict] = {}
    CONST: Dict[str, float] = {}

    # ---- FF
    for _, r in ff_df.iterrows():
        code = str(r["код"]).strip()
        name = str(r["наименование"]).strip()
        price = float(r["цена"])

        if not code or code.lower() == "nan":
            continue

        up = code.upper()

        if up.startswith("FF_SHIP_BOX_"):
            if name:
                FF_WAREHOUSES[name] = {"ship_per_box": price}
        else:
            key = up[3:] if up.startswith("FF_") else up
            CONST[key] = price

    # дефолты
    CONST.setdefault("PALLET", 0.0)
    CONST.setdefault("STRETCH", 0.0)
    CONST.setdefault("PALLET_THRESHOLD_BOXES", 10.0)
    CONST.setdefault("TO_TK_PER_BOX", 100.0)

    # ---- TK (только TK_... создаёт склады)
    for _, r in tk_df.iterrows():
        code = str(r["код"]).strip()
        name = str(r["наименование"]).strip()
        price = float(r["цена"])

        if not code or code.lower() == "nan":
            continue

        up = code.upper()

        # можно хранить константу и на листе TK, но это НЕ склад
        if up in ["TO_TK_PER_BOX", "TK_TO_TK_PER_BOX"]:
            CONST["TO_TK_PER_BOX"] = price
            continue

        # строгий фильтр складов на TK-листе
        if not up.startswith("TK_"):
            continue

        if not name:
            continue

        wh = name
        if wh not in TK_WAREHOUSES:
            TK_WAREHOUSES[wh] = {
                "to_warehouse_per_box": None,
                "to_warehouse_per_pallet": None,
                "entry_fee": 0.0,
            }

        if ("PALLET" in up) or ("ПАЛЛЕТ" in up):
            TK_WAREHOUSES[wh]["to_warehouse_per_pallet"] = price
        elif ("ENTRY" in up) or ("ВЪЕЗД" in up):
            TK_WAREHOUSES[wh]["entry_fee"] = price
        else:
            TK_WAREHOUSES[wh]["to_warehouse_per_box"] = price

    return FF_WAREHOUSES, TK_WAREHOUSES, CONST

# =========================
# DATA LOAD
# =========================

SERVICES = load_services_from_xlsx(SERVICES_XLSX)
FF_WAREHOUSES, TK_WAREHOUSES, CONST = load_shipping_from_xlsx(SHIPPING_XLSX)

WAREHOUSE_NAMES = sorted(set(list(FF_WAREHOUSES.keys()) + list(TK_WAREHOUSES.keys())))
if not WAREHOUSE_NAMES:
    raise RuntimeError("Не найдено ни одного склада ни на листе FF, ни на листе TK в shipping.xlsx")

# =========================
# FSM
# =========================

class CalcStates(StatesGroup):
    WaitingSkuName = State()
    WaitingSkuQty = State()
    WaitingSizeType = State()
    WaitingExactSize = State()
    WaitingTemplateSize = State()
    WaitingServices = State()
    WaitingMoreSku = State()
    WaitingWarehouse = State()
    WaitingBoxesOwner = State()
    WaitingWarehouseBoxChoice = State()
    WaitingClientBoxDims = State()
    ConfirmSend = State()

# =========================
# KEYBOARDS
# =========================

KB_YES_NO = types.ReplyKeyboardMarkup(resize_keyboard=True)
KB_YES_NO.add("Да", "Нет")

KB_BACK_ONLY = types.ReplyKeyboardMarkup(resize_keyboard=True)
KB_BACK_ONLY.add("Назад")

KB_SIZE_TYPE = types.ReplyKeyboardMarkup(resize_keyboard=True)
KB_SIZE_TYPE.add("Точные габариты", "Типовой размер")
KB_SIZE_TYPE.add("Назад")

KB_TEMPLATE_SIZES = types.ReplyKeyboardMarkup(resize_keyboard=True)
KB_TEMPLATE_SIZES.add("Маленький", "Средний", "Крупный")
KB_TEMPLATE_SIZES.add("Назад")

KB_WAREHOUSES = types.ReplyKeyboardMarkup(resize_keyboard=True)
for w in WAREHOUSE_NAMES:
    KB_WAREHOUSES.add(w)
KB_WAREHOUSES.add("Назад")

KB_BOX_OWNER = types.ReplyKeyboardMarkup(resize_keyboard=True)
KB_BOX_OWNER.add("Короба склада", "Короба клиента")
KB_BOX_OWNER.add("Назад")

KB_WAREHOUSE_BOX = types.ReplyKeyboardMarkup(resize_keyboard=True)
KB_WAREHOUSE_BOX.add("60×40×40", "40×30×30")
KB_WAREHOUSE_BOX.add("Назад")

KB_NEW_CALC = types.ReplyKeyboardMarkup(resize_keyboard=True)
KB_NEW_CALC.add("Новый расчёт")

def services_keyboard(cat: str) -> types.ReplyKeyboardMarkup:
    cat_low = (cat or "работа").strip().lower()
    if cat_low not in ["работа", "расходники"]:
        cat_low = "работа"

    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add("Работа", "Расходники")

    for name, (nm, price, c, unit) in SERVICES.items():
        if c != cat_low:
            continue
        kb.add(f"{nm} — {price:g} ₽/{unit}")

    kb.add("Отменить последнюю услугу")
    kb.add("Назад")
    kb.add("Готово")
    return kb

# =========================
# HELPERS
# =========================

def parse_dimensions(text: str) -> Optional[Tuple[float, float, float]]:
    t = text.lower().replace("×", "x").replace("*", "x").replace("х", "x")
    t = t.replace(",", ".").strip()
    if "x" in t:
        parts = [p.strip() for p in t.split("x") if p.strip()]
    else:
        parts = [p.strip() for p in t.split() if p.strip()]
    if len(parts) != 3:
        return None
    try:
        l, w, h = map(float, parts)
        if l <= 0 or w <= 0 or h <= 0:
            return None
        return l, w, h
    except Exception:
        return None

def get_template_dimensions(name: str):
    n = name.lower()
    if "мал" in n:
        return 10.0, 10.0, 5.0
    if "сред" in n:
        return 25.0, 20.0, 5.0
    if "круп" in n:
        return 30.0, 40.0, 5.0
    return None

def parse_service_button(text: str) -> str:
    # "Название — 5 ₽/шт" -> "Название"
    if "—" not in text:
        return ""
    return text.split("—", 1)[0].strip()

def vol(d: Tuple[float, float, float]) -> float:
    return float(d[0]) * float(d[1]) * float(d[2])

def boxes_per_pallet_by_volume(box_dims: Tuple[float, float, float]) -> int:
    pallet_volume = BASE_BOXES_PER_PALLET * vol(BASE_PALLET_BOX)  # объём 16 коробов 60×40×40
    box_volume = vol(box_dims)
    if box_volume <= 0:
        return BASE_BOXES_PER_PALLET
    m = int(math.floor(pallet_volume / box_volume))
    return max(1, m)

def calculate_boxes_for_item(qty: float, item_dims: Tuple[float, float, float], box_dims: Tuple[float, float, float]) -> Tuple[int, bool, float]:
    l, w, h = item_dims
    bl, bw, bh = box_dims

    is_oversize = (l > bl or w > bw or h > bh)
    if is_oversize:
        return int(math.ceil(qty)), True, 1.0

    item_volume = vol(item_dims)
    box_volume = vol(box_dims)

    if item_volume <= 0:
        items_per_box = 1.0
    else:
        items_per_box = box_volume / item_volume
        if items_per_box < 1:
            items_per_box = 1.0

    boxes = int(math.ceil(qty / items_per_box))
    return boxes, False, items_per_box

def party_palletization(total_boxes: int, box_dims: Tuple[float, float, float]) -> Tuple[int, int, float, float]:
    """
    Возвращает:
      pallets, max_boxes_on_pallet, pallet_cost, stretch_cost
    """
    threshold = float(CONST.get("PALLET_THRESHOLD_BOXES", 10.0))
    pallet_price = float(CONST.get("PALLET", 0.0))
    stretch_price = float(CONST.get("STRETCH", 0.0))

    max_on_pallet = boxes_per_pallet_by_volume(box_dims)

    if total_boxes < threshold:
        return 0, max_on_pallet, 0.0, 0.0

    pallets = int(math.ceil(total_boxes / max_on_pallet))
    pallet_cost = pallets * pallet_price
    stretch_cost = pallets * stretch_price
    return pallets, max_on_pallet, pallet_cost, stretch_cost

def party_shipping_cost(warehouse_name: str, total_boxes: int, pallets: int) -> Tuple[str, float, str]:
    """
    Авто:
      - если склад есть в TK_WAREHOUSES -> TK
      - иначе -> FF
    """
    if warehouse_name in TK_WAREHOUSES:
        tk = TK_WAREHOUSES[warehouse_name]
        to_tk_per_box = float(CONST.get("TO_TK_PER_BOX", 100.0))
        to_tk = total_boxes * to_tk_per_box
        entry = float(tk.get("entry_fee") or 0.0)

        if pallets > 0 and tk.get("to_warehouse_per_pallet") is not None:
            rate_p = float(tk["to_warehouse_per_pallet"])
            to_wh = pallets * rate_p
            mid = f"до склада: {pallets} паллет × {rate_p:g} ₽ = {to_wh:.2f} ₽"
        else:
            if tk.get("to_warehouse_per_box") is None:
                raise RuntimeError("Лист TK: нет ставки до склада (ни за короб, ни за паллету) для выбранного склада.")
            rate_b = float(tk["to_warehouse_per_box"])
            to_wh = total_boxes * rate_b
            mid = f"до склада: {total_boxes} короб × {rate_b:g} ₽ = {to_wh:.2f} ₽"

        cost = to_tk + to_wh + entry
        details = (
            f"Доставка (ТК): до ТК {total_boxes}×{to_tk_per_box:g}={to_tk:.2f} ₽ + "
            f"{mid} + въезд {entry:.2f} ₽ = {cost:.2f} ₽"
        )
        return "tk", cost, details

    if warehouse_name not in FF_WAREHOUSES:
        raise RuntimeError("Склад не найден ни на листе TK, ни на листе FF.")

    ship_per_box = float(FF_WAREHOUSES[warehouse_name]["ship_per_box"])
    cost = total_boxes * ship_per_box
    details = f"Доставка (FF): {total_boxes} короб × {ship_per_box:g} ₽ = {cost:.2f} ₽"
    return "ff", cost, details

def compute_sku_cost(sku: dict, box_dims: Tuple[float, float, float], use_client_boxes: bool) -> dict:
    qty = float(sku["qty"])
    item_dims = sku["dimensions"]

    boxes, is_oversize, items_per_box = calculate_boxes_for_item(qty, item_dims, box_dims)

    work_items = []
    cons_items = []
    work_per_unit = 0.0
    cons_per_unit = 0.0

    for svc_name in sku["services_names"]:
        if svc_name not in SERVICES:
            continue
        name, price, cat, unit = SERVICES[svc_name]
        row = {"name": name, "price": float(price), "unit": unit, "cat": cat}
        if cat == "расходники":
            cons_items.append(row)
            cons_per_unit += float(price)
        else:
            work_items.append(row)
            work_per_unit += float(price)

    work_total = work_per_unit * qty
    cons_total = cons_per_unit * qty

    boxes_cost = 0.0
    if (not use_client_boxes) and (not is_oversize):
        boxes_cost = float(boxes) * BOX_PRICE

    return {
        "qty": qty,
        "item_dims": item_dims,
        "boxes": int(boxes),
        "items_per_box": items_per_box,
        "is_oversize": is_oversize,
        "work_items": work_items,
        "cons_items": cons_items,
        "work_per_unit": work_per_unit,
        "cons_per_unit": cons_per_unit,
        "work_total": work_total,
        "cons_total": cons_total,
        "boxes_cost": boxes_cost,
        "sku_total_no_ship": work_total + cons_total + boxes_cost,
    }

def format_report(data: dict) -> str:
    skus = data["skus"]
    warehouse_name = data["warehouse"]
    use_client_boxes = data.get("use_client_boxes", False)
    box_dims = data["box_dims"]

    ship_label = "Через ТК" if warehouse_name in TK_WAREHOUSES else "Силами FF"

    lines: List[str] = []
    lines.append("⚙️ <b>FF Calc PRO</b> — расчёт партии до 10 SKU")
    lines.append(f"Склад: <b>{warehouse_name}</b>")
    lines.append(f"Доставка: <b>{ship_label}</b>")
    lines.append(f"Короба: <b>{'клиента' if use_client_boxes else 'склада'}</b>")
    lines.append(f"Размер короба: <b>{box_dims[0]:g}×{box_dims[1]:g}×{box_dims[2]:g}</b> см")
    lines.append("")

    total_sum_no_ship = 0.0
    total_boxes = 0
    oversize_any = False

    sku_calcs = []
    for i, sku in enumerate(skus, start=1):
        calc = compute_sku_cost(sku, box_dims, use_client_boxes)
        sku_calcs.append((i, sku, calc))
        total_sum_no_ship += calc["sku_total_no_ship"]
        total_boxes += int(calc["boxes"])
        if calc["is_oversize"]:
            oversize_any = True

    pallets, max_on_pallet, pallet_cost, stretch_cost = party_palletization(total_boxes, box_dims)
    ship_type, ship_cost, ship_details = party_shipping_cost(warehouse_name, total_boxes, pallets)

    party_total = total_sum_no_ship + pallet_cost + stretch_cost + ship_cost

    for i, sku, calc in sku_calcs:
        dims = calc["item_dims"]
        oversize_note = " (КГТ — условия уточнит менеджер)" if calc["is_oversize"] else ""

        lines.append(f"<b>Товар #{i}</b>{oversize_note}")
        lines.append(f"Название: {sku['name']}")
        lines.append(f"Количество: {calc['qty']:g} шт.")
        lines.append(f"Габариты: {dims[0]:g}×{dims[1]:g}×{dims[2]:g} см")

        if calc["work_items"] or calc["cons_items"]:
            lines.append("Услуги:")
            if calc["work_items"]:
                lines.append("  <b>Работа</b>:")
                for it in calc["work_items"]:
                    lines.append(f"   • {it['name']} — {it['price']:g} ₽/{it['unit']}")
            if calc["cons_items"]:
                lines.append("  <b>Расходники</b>:")
                for it in calc["cons_items"]:
                    lines.append(f"   • {it['name']} — {it['price']:g} ₽/{it['unit']}")
        else:
            lines.append("Услуги: Без услуг")

        lines.append(f"Работа: {calc['work_per_unit']:g} ₽/шт → {calc['work_total']:.2f} ₽")
        lines.append(f"Расходники: {calc['cons_per_unit']:g} ₽/шт → {calc['cons_total']:.2f} ₽")
        lines.append(f"Короба: {calc['boxes']} шт → {calc['boxes_cost']:.2f} ₽")
        lines.append(f"Подитог SKU (без доставки): <b>{calc['sku_total_no_ship']:.2f} ₽</b>")
        lines.append("")

    threshold = float(CONST.get("PALLET_THRESHOLD_BOXES", 10.0))
    lines.append(f"Коробов по партии: <b>{total_boxes}</b>")
    lines.append(f"Макс. коробов на паллете (по объёму): <b>{max_on_pallet}</b>")

    if total_boxes < threshold:
        lines.append(f"Паллетизация: не требуется (порог {threshold:g} короб.)")
    else:
        lines.append(f"Паллетизация: <b>{pallets}</b> паллет → паллеты {pallet_cost:.2f} ₽ + стретч {stretch_cost:.2f} ₽")

    lines.append(ship_details)

    if oversize_any:
        lines.append("⚠️ В партии есть КГТ. Для КГТ упаковка/отгрузка подтверждается менеджером.")

    lines.append("")
    lines.append(f"<b>ИТОГО партия: {party_total:.2f} ₽</b>")
    lines.append("Расчёт ориентировочный. Финальные условия подтверждает менеджер.")
    return "\n".join(lines)

def format_manager_request(user: types.User, data: dict) -> str:
    skus = data["skus"]
    warehouse_name = data["warehouse"]
    use_client_boxes = data.get("use_client_boxes", False)
    box_dims = data["box_dims"]

    ship_label = "Через ТК" if warehouse_name in TK_WAREHOUSES else "Силами FF"

    total_sum_no_ship = 0.0
    total_boxes = 0

    lines: List[str] = []
    lines.append("🆕 <b>Заявка с FF Calc PRO</b>")
    lines.append(f"Клиент: @{user.username or 'без username'} (id: {user.id})")
    lines.append(f"Склад: <b>{warehouse_name}</b>")
    lines.append(f"Доставка: <b>{ship_label}</b>")
    lines.append(f"Короба: <b>{'клиента' if use_client_boxes else 'склада'}</b>")
    lines.append(f"Размер короба: <b>{box_dims[0]:g}×{box_dims[1]:g}×{box_dims[2]:g}</b> см")
    lines.append("")

    sku_details = []
    for i, sku in enumerate(skus, start=1):
        calc = compute_sku_cost(sku, box_dims, use_client_boxes)
        total_sum_no_ship += calc["sku_total_no_ship"]
        total_boxes += int(calc["boxes"])

        dims = calc["item_dims"]
        block: List[str] = []
        block.append(f"<b>SKU #{i}</b>: {sku['name']}")
        block.append(f"Кол-во: {calc['qty']:g} шт | Габариты: {dims[0]:g}×{dims[1]:g}×{dims[2]:g} см")
        block.append(f"Короба: {calc['boxes']} шт | Короба(₽): {calc['boxes_cost']:.2f} ₽")
        block.append("Услуги:")

        if calc["work_items"]:
            block.append(" • <b>Работа</b>")
            for it in calc["work_items"]:
                block.append(f"    - {it['name']} — {it['price']:g} ₽/{it['unit']}")
        if calc["cons_items"]:
            block.append(" • <b>Расходники</b>")
            for it in calc["cons_items"]:
                block.append(f"    - {it['name']} — {it['price']:g} ₽/{it['unit']}")

        if not calc["work_items"] and not calc["cons_items"]:
            block.append(" • Без услуг")

        block.append(f"Итог работа: {calc['work_total']:.2f} ₽ | расходники: {calc['cons_total']:.2f} ₽")
        block.append(f"Подитог SKU (без доставки): <b>{calc['sku_total_no_ship']:.2f} ₽</b>")
        sku_details.append("\n".join(block))

    lines.append("\n\n".join(sku_details))
    lines.append("")

    pallets, max_on_pallet, pallet_cost, stretch_cost = party_palletization(total_boxes, box_dims)
    ship_type, ship_cost, ship_details = party_shipping_cost(warehouse_name, total_boxes, pallets)
    party_total = total_sum_no_ship + pallet_cost + stretch_cost + ship_cost

    threshold = float(CONST.get("PALLET_THRESHOLD_BOXES", 10.0))
    lines.append(f"Коробов по партии: <b>{total_boxes}</b>")
    lines.append(f"Макс. коробов/паллета (по объёму): <b>{max_on_pallet}</b>")
    if total_boxes >= threshold:
        lines.append(f"Паллет: <b>{pallets}</b> | паллеты {pallet_cost:.2f} ₽ | стретч {stretch_cost:.2f} ₽")
    else:
        lines.append(f"Паллетизация не требуется (порог {threshold:g})")
    lines.append(ship_details)
    lines.append("")
    lines.append(f"<b>ИТОГО партия: {party_total:.2f} ₽</b>")
    lines.append("Просьба связаться с клиентом и подтвердить финальные условия.")
    return "\n".join(lines)

# =========================
# HANDLERS
# =========================

@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message, state: FSMContext):
    await state.finish()
    await state.update_data(
        skus=[],
        current_sku=None,
        current_services_cat="работа",
        warehouse=None,
        use_client_boxes=False,
        box_dims=None,
    )
    await CalcStates.WaitingSkuName.set()

    await message.answer(
        "👋 Привет! Это <b>FF Calc PRO</b>.\n\n"
        "Считает партию до 10 SKU.\n"
        "Услуги: <b>Работа</b> / <b>Расходники</b>.\n"
        "Склады: в выборе только реальные склады (константы скрыты).\n\n"
        "Напиши название <b>товара #1</b>."
    )

@dp.message_handler(lambda m: m.text == "Новый расчёт")
async def new_calc(message: types.Message, state: FSMContext):
    await cmd_start(message, state)

@dp.message_handler(state=CalcStates.WaitingSkuName, content_types=types.ContentTypes.TEXT)
async def sku_name(message: types.Message, state: FSMContext):
    name = message.text.strip()
    if not name:
        await message.answer("Введи корректное название товара.")
        return

    await state.update_data(current_sku={
        "name": name,
        "qty": None,
        "dimensions": None,
        "services_names": [],
    })
    await CalcStates.WaitingSkuQty.set()
    await message.answer(
        f"Название: <b>{name}</b>\nВведи количество (шт.) числом.",
        reply_markup=KB_BACK_ONLY
    )

@dp.message_handler(state=CalcStates.WaitingSkuQty, content_types=types.ContentTypes.TEXT)
async def sku_qty(message: types.Message, state: FSMContext):
    text = message.text.strip()

    if text.lower() == "назад":
        await CalcStates.WaitingSkuName.set()
        await message.answer("Ок. Напиши название товара ещё раз.", reply_markup=types.ReplyKeyboardRemove())
        return

    text = text.replace(",", ".")
    try:
        qty = float(text)
        if qty <= 0:
            raise ValueError
    except Exception:
        await message.answer("Количество должно быть положительным числом. Попробуй ещё раз.", reply_markup=KB_BACK_ONLY)
        return

    data = await state.get_data()
    current_sku = data["current_sku"]
    current_sku["qty"] = qty
    await state.update_data(current_sku=current_sku)

    await CalcStates.WaitingSizeType.set()
    await message.answer(
        "Как зададим габариты?\n\n"
        "— <b>Точные габариты</b> (10x10x5)\n"
        "— <b>Типовой размер</b> (маленький/средний/крупный)\n",
        reply_markup=KB_SIZE_TYPE
    )

@dp.message_handler(state=CalcStates.WaitingSizeType, content_types=types.ContentTypes.TEXT)
async def size_type(message: types.Message, state: FSMContext):
    text = message.text.strip().lower()

    if text == "назад":
        await CalcStates.WaitingSkuQty.set()
        await message.answer("Ок. Введи количество числом.", reply_markup=KB_BACK_ONLY)
        return

    if "точн" in text:
        await CalcStates.WaitingExactSize.set()
        await message.answer(
            "Введи габариты в см: <code>длина x ширина x высота</code>\nПример: <code>10x10x5</code>",
            reply_markup=KB_BACK_ONLY
        )
        return

    if "типов" in text:
        await CalcStates.WaitingTemplateSize.set()
        await message.answer(
            "Выбери типовой размер:\n\n"
            "Маленький — 10×10×5\n"
            "Средний — 25×20×5\n"
            "Крупный — 30×40×5",
            reply_markup=KB_TEMPLATE_SIZES
        )
        return

    await message.answer("Выбери вариант кнопкой.", reply_markup=KB_SIZE_TYPE)

@dp.message_handler(state=CalcStates.WaitingExactSize, content_types=types.ContentTypes.TEXT)
async def exact_size(message: types.Message, state: FSMContext):
    text = message.text.strip()

    if text.lower() == "назад":
        await CalcStates.WaitingSizeType.set()
        await message.answer("Ок. Выбери тип габаритов.", reply_markup=KB_SIZE_TYPE)
        return

    dims = parse_dimensions(text)
    if not dims:
        await message.answer("Не распознал габариты. Пример: 10x10x5", reply_markup=KB_BACK_ONLY)
        return

    data = await state.get_data()
    current_sku = data["current_sku"]
    current_sku["dimensions"] = dims
    await state.update_data(current_sku=current_sku, current_services_cat="работа")

    await CalcStates.WaitingServices.set()
    await message.answer(
        "Габариты зафиксированы.\n\n"
        "Выбирай услуги. Переключай категории сверху.\n"
        "Когда закончишь — <b>Готово</b>.",
        reply_markup=services_keyboard("работа")
    )

@dp.message_handler(state=CalcStates.WaitingTemplateSize, content_types=types.ContentTypes.TEXT)
async def template_size(message: types.Message, state: FSMContext):
    text = message.text.strip()

    if text.lower() == "назад":
        await CalcStates.WaitingSizeType.set()
        await message.answer("Ок. Выбери тип габаритов.", reply_markup=KB_SIZE_TYPE)
        return

    dims = get_template_dimensions(text)
    if not dims:
        await message.answer("Выбери размер кнопкой.", reply_markup=KB_TEMPLATE_SIZES)
        return

    data = await state.get_data()
    current_sku = data["current_sku"]
    current_sku["dimensions"] = dims
    await state.update_data(current_sku=current_sku, current_services_cat="работа")

    await CalcStates.WaitingServices.set()
    await message.answer(
        "Размер зафиксирован.\n\n"
        "Выбирай услуги. Переключай категории сверху.\n"
        "Когда закончишь — <b>Готово</b>.",
        reply_markup=services_keyboard("работа")
    )

@dp.message_handler(state=CalcStates.WaitingServices, content_types=types.ContentTypes.TEXT)
async def services(message: types.Message, state: FSMContext):
    text = message.text.strip()
    data = await state.get_data()
    current_sku = data["current_sku"]
    cat = (data.get("current_services_cat") or "работа").lower()
    selected: List[str] = current_sku.get("services_names", [])

    if text.lower() in ["работа", "расходники"]:
        new_cat = text.lower()
        await state.update_data(current_services_cat=new_cat)
        await message.answer(f"Категория: <b>{text}</b>", reply_markup=services_keyboard(new_cat))
        return

    if text == "Отменить последнюю услугу":
        if selected:
            selected.pop()
        current_sku["services_names"] = selected
        await state.update_data(current_sku=current_sku)

        chosen = "\n".join([f"✅ {n}" for n in selected]) if selected else "Пока ничего не выбрано."
        await message.answer(
            f"Ок.\n\nСейчас выбрано:\n{chosen}\n\nПродолжай или жми «Готово».",
            reply_markup=services_keyboard(cat)
        )
        return

    if text == "Назад":
        await CalcStates.WaitingSizeType.set()
        await message.answer("Ок. Вернулись к выбору габаритов.", reply_markup=KB_SIZE_TYPE)
        return

    if text == "Готово":
        skus = data.get("skus", [])
        skus.append(current_sku)
        await state.update_data(skus=skus, current_sku=None, current_services_cat="работа")

        count = len(skus)
        if count >= MAX_SKU:
            await CalcStates.WaitingWarehouse.set()
            await message.answer(
                f"Добавлено {count} товаров (максимум).\n\nВыбери склад:",
                reply_markup=KB_WAREHOUSES
            )
            return

        await CalcStates.WaitingMoreSku.set()
        await message.answer(
            f"Товар сохранён. Сейчас в партии {count} позиций.\n\nДобавить ещё товар?",
            reply_markup=KB_YES_NO
        )
        return

    svc_name = parse_service_button(text)
    if not svc_name or svc_name not in SERVICES:
        await message.answer("Не понял услугу. Нажимай кнопки услуг.", reply_markup=services_keyboard(cat))
        return

    if svc_name not in selected:
        selected.append(svc_name)
    current_sku["services_names"] = selected
    await state.update_data(current_sku=current_sku)

    chosen = "\n".join([f"✅ {n}" for n in selected]) if selected else "Пока ничего не выбрано."
    await message.answer(
        f"Услуга добавлена.\n\nСейчас выбрано:\n{chosen}\n\nПродолжай или жми «Готово».",
        reply_markup=services_keyboard(cat)
    )

@dp.message_handler(state=CalcStates.WaitingMoreSku, content_types=types.ContentTypes.TEXT)
async def more_sku(message: types.Message, state: FSMContext):
    text = message.text.strip().lower()
    data = await state.get_data()
    skus = data.get("skus", [])
    count = len(skus)

    if text.startswith("д"):
        await CalcStates.WaitingSkuName.set()
        await message.answer(
            f"Ок. Напиши название <b>товара #{count + 1}</b>.",
            reply_markup=types.ReplyKeyboardRemove()
        )
        return

    if text.startswith("н"):
        await CalcStates.WaitingWarehouse.set()
        await message.answer("Выбери склад:", reply_markup=KB_WAREHOUSES)
        return

    await message.answer("Ответь «Да» или «Нет».", reply_markup=KB_YES_NO)

@dp.message_handler(state=CalcStates.WaitingWarehouse, content_types=types.ContentTypes.TEXT)
async def warehouse(message: types.Message, state: FSMContext):
    warehouse_name = message.text.strip()

    if warehouse_name.lower() == "назад":
        await CalcStates.WaitingMoreSku.set()
        await message.answer("Ок. Добавить ещё товар?", reply_markup=KB_YES_NO)
        return

    if warehouse_name not in WAREHOUSE_NAMES:
        await message.answer("Выбери склад из списка.", reply_markup=KB_WAREHOUSES)
        return

    await state.update_data(warehouse=warehouse_name)
    await CalcStates.WaitingBoxesOwner.set()

    ship_label = "Через ТК" if warehouse_name in TK_WAREHOUSES else "Силами FF"
    await message.answer(
        f"Ок. Доставка будет считаться: <b>{ship_label}</b>\n\nТеперь выбери короба:",
        reply_markup=KB_BOX_OWNER
    )

@dp.message_handler(state=CalcStates.WaitingBoxesOwner, content_types=types.ContentTypes.TEXT)
async def boxes_owner(message: types.Message, state: FSMContext):
    text = message.text.strip().lower()

    if text == "назад":
        await CalcStates.WaitingWarehouse.set()
        await message.answer("Ок. Выбери склад:", reply_markup=KB_WAREHOUSES)
        return

    if text.startswith("короба склада"):
        await state.update_data(use_client_boxes=False)
        await CalcStates.WaitingWarehouseBoxChoice.set()
        await message.answer("Выбери размер коробов склада:", reply_markup=KB_WAREHOUSE_BOX)
        return

    if text.startswith("короба клиента"):
        await state.update_data(use_client_boxes=True)
        await CalcStates.WaitingClientBoxDims.set()
        await message.answer(
            "Введи размер коробов клиента в см: <code>длина x ширина x высота</code>\nПример: <code>60x40x40</code>",
            reply_markup=KB_BACK_ONLY
        )
        return

    await message.answer("Выбери вариант кнопкой.", reply_markup=KB_BOX_OWNER)

@dp.message_handler(state=CalcStates.WaitingWarehouseBoxChoice, content_types=types.ContentTypes.TEXT)
async def warehouse_box_choice(message: types.Message, state: FSMContext):
    text = message.text.strip()

    if text.lower() == "назад":
        await CalcStates.WaitingBoxesOwner.set()
        await message.answer("Ок. Выбери короба:", reply_markup=KB_BOX_OWNER)
        return

    if text not in WAREHOUSE_BOXES:
        await message.answer("Выбери размер кнопкой.", reply_markup=KB_WAREHOUSE_BOX)
        return

    dims = WAREHOUSE_BOXES[text]
    await state.update_data(box_dims=dims)

    data = await state.get_data()
    report = format_report(data)

    await CalcStates.ConfirmSend.set()
    await message.answer(
        report + "\n\nОтправить заявку менеджеру?\nОтветь «Да» или «Нет».",
        reply_markup=KB_YES_NO
    )

@dp.message_handler(state=CalcStates.WaitingClientBoxDims, content_types=types.ContentTypes.TEXT)
async def client_box_dims(message: types.Message, state: FSMContext):
    text = message.text.strip()

    if text.lower() == "назад":
        await CalcStates.WaitingBoxesOwner.set()
        await message.answer("Ок. Выбери короба:", reply_markup=KB_BOX_OWNER)
        return

    dims = parse_dimensions(text)
    if not dims:
        await message.answer("Не распознал размер. Пример: 60x40x40", reply_markup=KB_BACK_ONLY)
        return

    await state.update_data(box_dims=dims)

    data = await state.get_data()
    report = format_report(data)

    await CalcStates.ConfirmSend.set()
    await message.answer(
        report + "\n\nОтправить заявку менеджеру?\nОтветь «Да» или «Нет».",
        reply_markup=KB_YES_NO
    )

@dp.message_handler(state=CalcStates.ConfirmSend, content_types=types.ContentTypes.TEXT)
async def confirm_send(message: types.Message, state: FSMContext):
    text = message.text.strip().lower()
    data = await state.get_data()

    if text.startswith("д"):
        req = format_manager_request(message.from_user, data)
        await bot.send_message(MANAGER_CHAT_ID, req)
        await message.answer(
            "Заявка отправлена менеджеру.\n\nЧтобы посчитать новую партию — нажми «Новый расчёт».",
            reply_markup=KB_NEW_CALC
        )
        await state.finish()
        return

    if text.startswith("н"):
        await message.answer(
            "Ок, заявку не отправляю.\nЧтобы посчитать новую партию — нажми «Новый расчёт».",
            reply_markup=KB_NEW_CALC
        )
        await state.finish()
        return

    await message.answer("Ответь «Да» или «Нет».", reply_markup=KB_YES_NO)

# =========================
# RUN
# =========================

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)