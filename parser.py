import asyncio
import logging
import os
import random
import json
import re
import sys
import time
from typing import Any

from faker import Faker
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError, Page

fake = Faker()


class ColorFormatter(logging.Formatter):
    COLORS = {
        logging.INFO: "\033[94m",  # Синий
        logging.WARNING: "\033[93m",  # Жёлтый
        logging.ERROR: "\033[91m"  # Красный
    }

    RESET = "\033[0m"

    def format(self, record):
        log_color = self.COLORS.get(record.levelno, self.RESET)
        message = super().format(record)
        return f"{log_color}{message}{self.RESET}"


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler('project_logging.log', encoding='utf-8')
file_handler.setFormatter(ColorFormatter('%(asctime)s - %(levelname)s: %(message)s'))
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(ColorFormatter('%(asctime)s - %(levelname)s: %(message)s'))
logger.addHandler(file_handler)
logger.addHandler(console_handler)


# ----- Класс отвечающий за вспомогательные функции
class HelpFunc:
    # ----- Генерация фейковых данных -----]
    @staticmethod
    def generate_fake_headers():
        return {
            "User-Agent": fake.user_agent(),
            "Accept-Language": fake.locale().replace("_", "-"),
            "Referer": fake.url(),
        }

    # ----- Рандомная задержка -----
    @staticmethod
    async def human_delay(min_ms: int = 200, max_ms: int = 750):
        await asyncio.sleep(random.uniform(min_ms / 1000, max_ms / 1000))

    # ----- Скролл страницы для "очеловечивания" -----
    @staticmethod
    async def human_scroll(page: Page):
        for _ in range(random.randint(2, 7)):
            await page.mouse.wheel(0, random.randint(300, 1200))
            await HelpFunc.human_delay(300, 900)

    # ----- Нахождение ключа без его фактического названия -----]
    @staticmethod
    async def find_value_by_partial_key(data: dict, part: str) -> dict | None:
        for k, v in data.items():
            if part.lower() in k.lower():
                return {k: v}
        return None

    # ----- Обработка полученных данных после парсинга и формирование результата -----]
    @staticmethod
    async def get_result_of_parsing(vehicle_name, specifications):
        ua_price_regular = None
        ua_price_disc = None
        eu_price_regular = None
        eu_price_disc = None
        for spec in specifications:
            ua_price_regular = await HelpFunc.find_value_by_partial_key(spec, 'регулярна ціна')
            ua_price_disc = await HelpFunc.find_value_by_partial_key(spec, 'акційна ціна')
            eu_price_regular = await HelpFunc.find_value_by_partial_key(spec, 'еквівалент регулярної ціни')
            eu_price_disc = await HelpFunc.find_value_by_partial_key(spec, 'еквівалент акційної ціни')
        return  {'model_name': vehicle_name, 'сonfigurations': specifications,
                  'price': {'EU': eu_price_regular, 'UAH': ua_price_regular},
                  'price_disc': {'EU': eu_price_disc, 'UAH': ua_price_disc}}

    # ----- Сохранение данных в словарь -----
    @staticmethod
    def save_dict_to_json(data: list, file_path: str = 'cars_data.json', folder_name: str = 'data'):
        os.makedirs(folder_name, exist_ok=True)
        file = f'{folder_name}/{file_path}'
        if not os.path.exists(file):
            with open(file, "w", encoding="utf-8") as f:
                f.write("{}")

        with open(file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logger.info('Файл с данными был успешно сохранён')



    # ----- Сохранение данных в словарь -----
    @staticmethod
    def periodic_run(hours = 12):
        while True:
            time.sleep(hours * 60 * 60)
            logger.info('Прошло 12 часов с момента последнего запуска. Перезапуск парсинга.')
            asyncio.run(main())


# ----- Класс для функций касающихся парсинга -----]
class Parsing:
    def __init__(self):
        self.main_page = None
        self.proxy = None
        self.cars_pages = None
        self.cars_info = list()

    # ----- Функция парсящая основную станицу -----]
    async def get_cars_links(self, retries: int = 3) -> bool:
        for attempt in range(1, retries + 1):
            try:
                print(f"Попытка №{attempt} открыть страницу...")

                async with async_playwright() as p:
                    chromium_args = []

                    if self.proxy:
                        browser_config = {"proxy": self.proxy}
                    else:
                        browser_config = {}

                    ua = fake.user_agent()
                    chromium_args.append(f"--user-agent={ua}")

                    browser = await p.chromium.launch(
                        headless=True,
                        args=chromium_args,
                        **browser_config
                    )

                    context = await browser.new_context(
                        user_agent=ua,
                        locale=fake.locale().replace("_", "-"),
                        extra_http_headers=HelpFunc.generate_fake_headers(),
                        viewport={"width": 1280, "height": 720},
                    )

                    page = await context.new_page()

                    # Отключаем webdriver
                    await context.add_init_script("""
                        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    """)

                    # Блокируем медиа-файлы для ускорения
                    async def block_media(route, request):
                        if request.resource_type in ["image", "media", "font", "stylesheet"]:
                            await route.abort()
                        else:
                            await route.continue_()

                    await page.route("**/*", block_media)

                    logger.info(f"Открытие: {self.main_page}")
                    try:
                        await page.goto(self.main_page, timeout=60000, wait_until='domcontentloaded')
                    except PlaywrightTimeoutError:
                        logger.warning(
                            f"Таймаут при загрузке {self.main_page}, продолжаем дальше")

                    self.cars_pages = await self.get_data_from_main_page(page)

                    await browser.close()
                    return True

            except PlaywrightTimeoutError:
                logger.error("Таймаут загрузки страницы. Пробуем ещё раз...")
            except Exception as e:
                logger.error(f"Ошибка: {e}, повторяю...")

            await asyncio.sleep(2)

        logger.error("Не удалось открыть страницу после всех попыток.")
        return False

    # ----- Получает главную страницу и вытягивает оттуда ссылки на доступные авто -----]
    @staticmethod
    async def get_data_from_main_page(page: Page) -> list[str]:
        await HelpFunc.human_delay(max_ms=1350)
        li_elements = await page.query_selector_all("div.car_grid li")
        links = []
        for li in li_elements:
            a_tag = await li.query_selector("a")
            if a_tag:
                href = await a_tag.get_attribute("href")
                links.append(href)
        logger.info("Ссылки на автомобили получены.")
        logger.info(f'Количество полученных автомобилей: {len(set(links))}')
        return list(set(links))

    # ----- Запуск батчинга полученных страниц автомобилей -----]
    async def process_cars_batching(self, batch_size: int = 3, proxy: dict | None = None):
        async with async_playwright() as p:
            chromium_args = []

            if proxy:
                browser_config = {"proxy": proxy}
            else:
                browser_config = {}

            ua = fake.user_agent()
            chromium_args.append(f"--user-agent={ua}")

            browser = await p.chromium.launch(
                headless=True,
                args=chromium_args,
                **browser_config
            )

            # функция для блокировки медиа
            async def block_media(route, request):
                if request.resource_type in ["image", "media", "font", "stylesheet"]:
                    await route.abort()
                else:
                    await route.continue_()

            for i in range(0, len(self.cars_pages), batch_size):
                batch = self.cars_pages[i:i + batch_size]
                logger.info(f"\n=== Батч {i // batch_size + 1}: {batch} ===")

                tasks = []
                contexts = []

                for car_url in batch:
                    context = await browser.new_context(
                        user_agent=ua,
                        viewport={"width": 1280, "height": 720},
                    )
                    page = await context.new_page()

                    # блокируем медиа
                    await page.route("**/*", block_media)

                    # отключаем webdriver
                    await context.add_init_script("""
                        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    """)

                    contexts.append(context)
                    tasks.append(self.process_car(page, car_url))

                await asyncio.gather(*tasks)

                # закрываем все контексты после обработки батча
                for context in contexts:
                    await context.close()

            await browser.close()

    # ----- Обработка одной страницы батчинга, для получения оттуда необходимой информации -----]
    async def process_car(self, page: Page, car_url: str):
        i = 1
        while i <= 3:
            try:
                await page.goto(f'https://www.kia.com{car_url.split(".html")[0]}/specification.html', timeout=60000,
                            wait_until='domcontentloaded')
                break
            except PlaywrightTimeoutError:
                logger.warning(f"Таймаут при загрузке https://www.kia.com{car_url.split('.html')[0]}/features.html, повторная попытка.")
                i+= 1
        else:
            logger.warning(f'Не получилось собрать данные со страницы https://www.kia.com{car_url.split('.html')[0]}/features.html. Пропуск')
            return
        logger.info(f'Получены основные данные для {car_url}')
        await HelpFunc.human_delay(max_ms=1350)
        vehicle_name, specifications = await self.get_specification(page)

        self.cars_info.append(await HelpFunc.get_result_of_parsing(vehicle_name, specifications))


    # ----- Парсинг из страницы features авто для кратких характеристик и названия -----]
    @staticmethod
    async def get_features(page: Page, car_url: str) -> dict[str, str]:
        i = 1
        # Получение кратких характеристик
        car_features = {}
        while True:
            li = await page.query_selector(f"li.infor{i}")
            if li is None:
                break
            info_title = await li.query_selector("span.inforTit")
            info_title = await info_title.inner_text() if info_title else None

            info_text = await li.query_selector("span.inforTxt")
            info_text = await info_text.inner_text() if info_text else None
            car_features[info_title] = info_text if info_text != '-' else None
            i += 1
        del i
        logger.info(
            f"Получено коротких технических характеристик для автомобиля {car_url.split('/')[3].split('.')[0]} - {len(car_features)}")
        await HelpFunc.human_scroll(page=page)
        await HelpFunc.human_delay(max_ms=1350)
        await HelpFunc.human_delay(max_ms=1350)
        return car_features

    # ----- Парсинг из страницы specification авто для комплектаций -----]
    @staticmethod
    async def get_specification(page: Page) ->  tuple[str, list]:
        data = []
        # Получение названий авто
        vehicle_name = ''
        scripts = await page.query_selector_all('script')
        for script in scripts:
            content = await script.text_content()
            if not content:
                continue
            match = re.search(r"'vehicle_name'\s*:\s*'([^']+)'", content)
            if match:
                vehicle_name = match.group(1).capitalize()
                break
        # Получение всех доступных спецификаций
        specifications = await page.query_selector_all('.parbase.spec_feature_list.section')
        if specifications:
            for spec_i, specification in enumerate(specifications):
                # Получение всех таблиц спецификации
                specification_titles = await specification.query_selector_all('h2.tit')
                table = await specification.query_selector_all('table')
                specification_names = []
                main_category_name = None
                info = {}
                if table:
                    for table_i, t in enumerate(table):
                        curr_spec = await specification_titles[table_i].inner_text()
                        info[curr_spec] = {}

                        # Получение названий спецификаций
                        if len(specification_names) == 0:
                            head = await t.query_selector('thead')
                            head_th = await head.query_selector_all('th')
                            for th in head_th:
                                if head_th[0] == th:
                                    continue
                                spec_name = await th.inner_text()
                                spec_name = spec_name.replace('\n', ' ')
                                specification_names.append(spec_name)
                                main_category_name = await head_th[0].inner_text()
                                info[curr_spec][spec_name] = {main_category_name: spec_name}
                        else:
                            for spec_name in specification_names:
                                info[curr_spec][spec_name] = {main_category_name: spec_name}
                        # Получение характеристик спецификации
                        body = await t.query_selector('tbody')
                        body_items = await body.query_selector_all('tr')
                        for body_item in body_items:
                            item_key = await body_item.query_selector('th')
                            if item_key is None:
                                continue
                            # Спецификации
                            item_key = await item_key.inner_text()
                            item_values = await body_item.query_selector_all('td')
                            for item_i, value in enumerate(item_values):
                                current_name = specification_names[item_i]
                                value = await value.inner_text()
                                value = value.strip()
                                match value:
                                    case '•':
                                        value = True
                                    case "●":
                                        value = True
                                    case '-':
                                        value = False
                                info[curr_spec][current_name][item_key] = value
                data.append(info)
        return vehicle_name, data


# ----- Точка входа -----]
async def main(url="https://www.kia.com/ua/main.html"):
    parsing = Parsing()
    parsing.main_page = url
    parsing.proxy = None
    while True:
        await parsing.get_cars_links()
        if not parsing.cars_pages:
            logger.info('Попытка запросить данные с KIA.com не увенчались успехом. Повторная попытка через 5 минут')
            await asyncio.sleep(5)
            continue
        break
    await parsing.process_cars_batching(batch_size=3)
    HelpFunc.save_dict_to_json(parsing.cars_info)


if __name__ == "__main__":
    asyncio.run(main())
    HelpFunc.periodic_run()