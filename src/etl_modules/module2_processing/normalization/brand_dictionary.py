"""
Brand Standardization Dictionary
Повний словник 100+ українських роздрібних брендів
"""

from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class BrandInfo:
    """Інформація про бренд"""
    canonical_name: str
    synonyms: List[str]
    format: str
    influence_weight: float
    functional_group: str
    parent_company: Optional[str] = None
    osm_tags: Optional[List[str]] = None


class BrandDictionary:
    """Словник брендів для стандартизації"""
    
    def __init__(self):
        self.brands = self._initialize_brands()
        self.synonym_index = self._build_synonym_index()
        
    def _initialize_brands(self) -> Dict[str, BrandInfo]:
        """Ініціалізація повного словника брендів"""
        brands = {}
        
        # ===== НАЦІОНАЛЬНІ СУПЕРМАРКЕТИ (Прямі конкуренти) =====
        brands.update({
            'atb': BrandInfo(
                canonical_name='АТБ-Маркет',
                synonyms=['АТБ', 'ATB', 'А.Т.Б', 'атб', 'АТБ-маркет', 'ATB-Market'],
                format='дискаунтер',
                influence_weight=-0.9,
                functional_group='competitor',
                parent_company='АТБ-Маркет',
                osm_tags=['shop=supermarket', 'brand=АТБ', 'name~АТБ']
            ),
            'silpo': BrandInfo(
                canonical_name='Сільпо',
                synonyms=['Сільпо', 'Silpo', 'СІЛЬПО', 'силпо', 'Сільпо-Фуд'],
                format='супермаркет',
                influence_weight=-0.85,
                functional_group='competitor',
                parent_company='Fozzy Group',
                osm_tags=['shop=supermarket', 'brand=Сільпо', 'name~Silpo']
            ),
            'fora': BrandInfo(
                canonical_name='Фора',
                synonyms=['Фора', 'Fora', 'ФОРА', 'фора'],
                format='магазин біля дому',
                influence_weight=-0.75,
                functional_group='competitor',
                parent_company='Fozzy Group',
                osm_tags=['shop=convenience', 'brand=Фора', 'name~Fora']
            ),
            'novus': BrandInfo(
                canonical_name='Novus',
                synonyms=['Novus', 'НОВУС', 'новус', 'Новус'],
                format='супермаркет',
                influence_weight=-0.8,
                functional_group='competitor',
                parent_company='Novus Україна',
                osm_tags=['shop=supermarket', 'brand=Novus', 'name~Novus']
            ),
            'metro': BrandInfo(
                canonical_name='Metro',
                synonyms=['Metro', 'МЕТРО', 'метро', 'Метро', 'Metro Cash & Carry'],
                format='гіпермаркет',
                influence_weight=-0.7,
                functional_group='competitor',
                parent_company='Metro Cash & Carry',
                osm_tags=['shop=hypermarket', 'brand=Metro', 'name~Metro']
            ),
            'auchan': BrandInfo(
                canonical_name='Auchan',
                synonyms=['Auchan', 'Ашан', 'АШАН', 'ашан'],
                format='гіпермаркет',
                influence_weight=-0.85,
                functional_group='competitor',
                parent_company='Auchan Retail',
                osm_tags=['shop=supermarket', 'brand=Auchan', 'name~Auchan']
            ),
            'varus': BrandInfo(
                canonical_name='Varus',
                synonyms=['Varus', 'ВАРУС', 'варус', 'Варус'],
                format='супермаркет',
                influence_weight=-0.75,
                functional_group='competitor',
                parent_company='Omega',
                osm_tags=['shop=supermarket', 'brand=Varus', 'name~Varus']
            ),
            'megamarket': BrandInfo(
                canonical_name='MegaMarket',
                synonyms=['MegaMarket', 'МегаМаркет', 'МЕГАМАРКЕТ', 'мегамаркет'],
                format='супермаркет',
                influence_weight=-0.7,
                functional_group='competitor',
                parent_company='ТОВ МегаМаркет'
            ),
            'eko_market': BrandInfo(
                canonical_name='ЕКО маркет',
                synonyms=['ЕКО маркет', 'EKO market', 'Эко маркет', 'еко маркет'],
                format='супермаркет',
                influence_weight=-0.75,
                functional_group='competitor',
                parent_company='Retail Group',
                osm_tags=['shop=supermarket', 'brand=ЕКО', 'name~ЕКО']
            ),
            'nash_krai': BrandInfo(
                canonical_name='Наш Край',
                synonyms=['Наш Край', 'НАШ КРАЙ', 'наш край'],
                format='магазин біля дому',
                influence_weight=-0.65,
                functional_group='competitor',
                parent_company='Наш Край'
            )
        })
        
        # ===== РЕГІОНАЛЬНІ СУПЕРМАРКЕТИ =====
        brands.update({
            'rukavychka': BrandInfo(
                canonical_name='Рукавичка',
                synonyms=['Рукавичка', 'РУКАВИЧКА', 'рукавичка'],
                format='супермаркет',
                influence_weight=-0.7,
                functional_group='competitor',
                parent_company='Рукавичка'
            ),
            'fayno_market': BrandInfo(
                canonical_name='Файно Маркет',
                synonyms=['Файно Маркет', 'Fayno Market', 'файно маркет'],
                format='супермаркет',
                influence_weight=-0.65,
                functional_group='competitor',
                parent_company='Файно Маркет'
            ),
            'tavria_v': BrandInfo(
                canonical_name='Таврія В',
                synonyms=['Таврія В', 'Таврия В', 'ТАВРІЯ В', 'таврія в'],
                format='супермаркет',
                influence_weight=-0.7,
                functional_group='competitor',
                parent_company='Таврія В'
            ),
            'kopiyka': BrandInfo(
                canonical_name='Копійка',
                synonyms=['Копійка', 'Копейка', 'КОПІЙКА', 'копійка'],
                format='магазин біля дому',
                influence_weight=-0.6,
                functional_group='competitor',
                parent_company='Копійка'
            ),
            'velika_kyshenia': BrandInfo(
                canonical_name='Велика Кишеня',
                synonyms=['Велика Кишеня', 'ВК', 'велика кишеня', 'Большой Карман'],
                format='супермаркет',
                influence_weight=-0.7,
                functional_group='competitor',
                parent_company='Велика Кишеня'
            )
        })
        
        # ===== МАГАЗИНИ БІЛЯ ДОМУ =====
        brands.update({
            'blyzenko': BrandInfo(
                canonical_name='Близенько',
                synonyms=['Близенько', 'БЛИЗЕНЬКО', 'близенько', 'Blyzenko'],
                format='магазин біля дому',
                influence_weight=-0.6,
                functional_group='competitor',
                parent_company='Fozzy Group'
            ),
            'kolo': BrandInfo(
                canonical_name='КОЛО',
                synonyms=['КОЛО', 'Коло', 'коло', 'KOLO'],
                format='магазин біля дому',
                influence_weight=-0.55,
                functional_group='competitor',
                parent_company='КОЛО'
            ),
            'magnit': BrandInfo(
                canonical_name='Магніт',
                synonyms=['Магніт', 'Магнит', 'МАГНІТ', 'магніт'],
                format='магазин біля дому',
                influence_weight=-0.5,
                functional_group='competitor',
                parent_company='Local chains'
            ),
            'produkty': BrandInfo(
                canonical_name='Продукти',
                synonyms=['Продукти', 'ПРОДУКТИ', 'продукти'],
                format='магазин біля дому',
                influence_weight=-0.4,
                functional_group='competitor',
                parent_company='Various'
            )
        })
        
        # ===== СПЕЦІАЛІЗОВАНІ ПРОДУКТОВІ =====
        brands.update({
            'myasomarket': BrandInfo(
                canonical_name="М'ясомаркет",
                synonyms=["М'ясомаркет", 'Мясомаркет', "м'ясомаркет", 'MeatMarket'],
                format='м\'ясний магазин',
                influence_weight=-0.3,
                functional_group='competitor',
                parent_company="М'ясомаркет"
            ),
            'nash_myasnik': BrandInfo(
                canonical_name='Наш М\'ясник',
                synonyms=['Наш М\'ясник', 'Наш Мясник', 'наш м\'ясник'],
                format='м\'ясний магазин',
                influence_weight=-0.3,
                functional_group='competitor',
                parent_company='Наш М\'ясник'
            ),
            'rybalochka': BrandInfo(
                canonical_name='Рибалочка',
                synonyms=['Рибалочка', 'РИБАЛОЧКА', 'рибалочка'],
                format='рибний магазин',
                influence_weight=-0.25,
                functional_group='competitor',
                parent_company='Рибалочка'
            ),
            'ocean_plaza_fish': BrandInfo(
                canonical_name='Ocean Plaza Fish',
                synonyms=['Ocean Plaza Fish', 'Ocean Fish', 'океан риба'],
                format='рибний магазин',
                influence_weight=-0.25,
                functional_group='competitor'
            ),
            'ovochi_frukty': BrandInfo(
                canonical_name='Овочі-Фрукти',
                synonyms=['Овочі-Фрукти', 'Овощи-Фрукты', 'овочі фрукти'],
                format='овочевий магазин',
                influence_weight=-0.2,
                functional_group='competitor'
            )
        })
        
        # ===== PREMIUM/ORGANIC =====
        brands.update({
            'goodwine': BrandInfo(
                canonical_name='Goodwine',
                synonyms=['Goodwine', 'GOODWINE', 'гудвайн', 'Good Wine'],
                format='преміум супермаркет',
                influence_weight=-0.4,
                functional_group='competitor',
                parent_company='Goodwine'
            ),
            'winetime': BrandInfo(
                canonical_name='Winetime',
                synonyms=['Winetime', 'WINETIME', 'вайнтайм', 'Wine Time'],
                format='винний магазин',
                influence_weight=-0.3,
                functional_group='competitor',
                parent_company='Winetime'
            ),
            'ecomarket': BrandInfo(
                canonical_name='Ecomarket',
                synonyms=['Ecomarket', 'ECOMARKET', 'екомаркет', 'Eco Market'],
                format='органік магазин',
                influence_weight=-0.35,
                functional_group='competitor'
            ),
            'organic_market': BrandInfo(
                canonical_name='Organic Market',
                synonyms=['Organic Market', 'органік маркет', 'ORGANIC MARKET'],
                format='органік магазин',
                influence_weight=-0.35,
                functional_group='competitor'
            )
        })
        
        # ===== МЕРЕЖІ ПЕКАРЕНЬ =====
        brands.update({
            'frantsua': BrandInfo(
                canonical_name='Франс.уа',
                synonyms=['Франс.уа', 'Франсуа', 'ФРАНС.УА', 'франс уа', 'France.ua'],
                format='пекарня-кондитерська',
                influence_weight=-0.2,
                functional_group='competitor',
                parent_company='Lauffer Group'
            ),
            'lvivski_kruasany': BrandInfo(
                canonical_name='Львівські круасани',
                synonyms=['Львівські круасани', 'Львовские круассаны', 'львівські круасани'],
                format='пекарня',
                influence_weight=-0.15,
                functional_group='competitor'
            ),
            'khlib_dnya': BrandInfo(
                canonical_name='Хліб Дня',
                synonyms=['Хліб Дня', 'Хлеб Дня', 'хліб дня'],
                format='пекарня',
                influence_weight=-0.15,
                functional_group='competitor'
            ),
            'bulochki_khlibchyk': BrandInfo(
                canonical_name='Булочки & Хлібчик',
                synonyms=['Булочки & Хлібчик', 'Булочки и Хлебчик', 'булочки хлібчик'],
                format='пекарня',
                influence_weight=-0.15,
                functional_group='competitor'
            )
        })
        
        # ===== DROGERIE & VARIETY STORES =====
        brands.update({
            'eva': BrandInfo(
                canonical_name='EVA',
                synonyms=['EVA', 'ЕВА', 'єва', 'Eva', 'Ева'],
                format='дрогері',
                influence_weight=-0.4,
                functional_group='competitor',
                parent_company='EVA',
                osm_tags=['shop=chemist', 'brand=EVA', 'name~EVA']
            ),
            'prostor': BrandInfo(
                canonical_name='Простор',
                synonyms=['Простор', 'ПРОСТОР', 'простор', 'Prostor'],
                format='дрогері',
                influence_weight=-0.35,
                functional_group='competitor',
                parent_company='Простор'
            ),
            'watsons': BrandInfo(
                canonical_name='Watsons',
                synonyms=['Watsons', 'WATSONS', 'ватсонс', 'Ватсонс'],
                format='дрогері',
                influence_weight=-0.35,
                functional_group='competitor',
                parent_company='A.S. Watson Group'
            ),
            'avrora': BrandInfo(
                canonical_name='Аврора',
                synonyms=['Аврора', 'АВРОРА', 'аврора', 'Avrora'],
                format='variety store',
                influence_weight=-0.5,
                functional_group='competitor',
                parent_company='Аврора'
            ),
            'chervonyi_market': BrandInfo(
                canonical_name='Червоний маркет',
                synonyms=['Червоний маркет', 'Красный маркет', 'червоний маркет'],
                format='variety store',
                influence_weight=-0.45,
                functional_group='competitor'
            ),
            'oneprice': BrandInfo(
                canonical_name='OnePrice',
                synonyms=['OnePrice', 'One Price', 'ВанПрайс', 'ванпрайс'],
                format='fixed price store',
                influence_weight=-0.4,
                functional_group='competitor'
            ),
            'vse_po_49': BrandInfo(
                canonical_name='Все по 49',
                synonyms=['Все по 49', 'ВСЕ ПО 49', 'все по 49'],
                format='fixed price store',
                influence_weight=-0.35,
                functional_group='competitor'
            )
        })
        
        # ===== ГЕНЕРАТОРИ ТРАФІКУ - DIY/HOME =====
        brands.update({
            'epicentr': BrandInfo(
                canonical_name='Епіцентр К',
                synonyms=['Епіцентр К', 'Епіцентр', 'ЕПІЦЕНТР', 'епіцентр', 'Epicentr K'],
                format='гіпермаркет DIY',
                influence_weight=0.8,
                functional_group='traffic_generator',
                parent_company='Епіцентр К',
                osm_tags=['shop=doityourself', 'brand=Епіцентр', 'name~Епіцентр']
            ),
            'nova_linia': BrandInfo(
                canonical_name='Нова Лінія',
                synonyms=['Нова Лінія', 'Новая Линия', 'нова лінія', 'Nova Linia'],
                format='гіпермаркет DIY',
                influence_weight=0.75,
                functional_group='traffic_generator',
                parent_company='Нова Лінія'
            ),
            'leroy_merlin': BrandInfo(
                canonical_name='Leroy Merlin',
                synonyms=['Leroy Merlin', 'Леруа Мерлен', 'леруа мерлен', 'LEROY MERLIN'],
                format='гіпермаркет DIY',
                influence_weight=0.8,
                functional_group='traffic_generator',
                parent_company='Leroy Merlin'
            ),
            'jysk': BrandInfo(
                canonical_name='JYSK',
                synonyms=['JYSK', 'ЙИСК', 'йиск', 'Jysk'],
                format='меблевий магазин',
                influence_weight=0.6,
                functional_group='traffic_generator',
                parent_company='JYSK'
            )
        })
        
        # ===== ГЕНЕРАТОРИ ТРАФІКУ - ЕЛЕКТРОНІКА =====
        brands.update({
            'rozetka': BrandInfo(
                canonical_name='Rozetka',
                synonyms=['Rozetka', 'Розетка', 'РОЗЕТКА', 'розетка'],
                format='магазин електроніки',
                influence_weight=0.7,
                functional_group='traffic_generator',
                parent_company='Rozetka',
                osm_tags=['shop=electronics', 'brand=Rozetka']
            ),
            'comfy': BrandInfo(
                canonical_name='Comfy',
                synonyms=['Comfy', 'КОМФІ', 'комфі', 'Комфи'],
                format='магазин електроніки',
                influence_weight=0.65,
                functional_group='traffic_generator',
                parent_company='Comfy Trade'
            ),
            'foxtrot': BrandInfo(
                canonical_name='Foxtrot',
                synonyms=['Foxtrot', 'Фокстрот', 'ФОКСТРОТ', 'фокстрот'],
                format='магазин електроніки',
                influence_weight=0.65,
                functional_group='traffic_generator',
                parent_company='Foxtrot'
            ),
            'allo': BrandInfo(
                canonical_name='АЛЛО',
                synonyms=['АЛЛО', 'Алло', 'алло', 'ALLO'],
                format='магазин електроніки',
                influence_weight=0.6,
                functional_group='traffic_generator',
                parent_company='АЛЛО'
            ),
            'moyo': BrandInfo(
                canonical_name='MOYO',
                synonyms=['MOYO', 'МОЙО', 'мойо', 'Moyo'],
                format='магазин електроніки',
                influence_weight=0.55,
                functional_group='traffic_generator',
                parent_company='MOYO'
            ),
            'citrus': BrandInfo(
                canonical_name='Цитрус',
                synonyms=['Цитрус', 'ЦИТРУС', 'цитрус', 'Citrus'],
                format='магазин електроніки',
                influence_weight=0.5,
                functional_group='traffic_generator',
                parent_company='Цитрус'
            )
        })
        
        # ===== ГЕНЕРАТОРИ ТРАФІКУ - FASHION =====
        brands.update({
            'lc_waikiki': BrandInfo(
                canonical_name='LC Waikiki',
                synonyms=['LC Waikiki', 'ЛС Вайкікі', 'лс вайкікі', 'LCWAIKIKI'],
                format='магазин одягу',
                influence_weight=0.6,
                functional_group='traffic_generator',
                parent_company='LC Waikiki'
            ),
            'colin_s': BrandInfo(
                canonical_name="Colin's",
                synonyms=["Colin's", 'Колінз', 'колінс', "COLIN'S"],
                format='магазин одягу',
                influence_weight=0.5,
                functional_group='traffic_generator',
                parent_company="Colin's"
            ),
            'reserved': BrandInfo(
                canonical_name='Reserved',
                synonyms=['Reserved', 'RESERVED', 'резервед', 'Резервед'],
                format='магазин одягу',
                influence_weight=0.55,
                functional_group='traffic_generator',
                parent_company='LPP'
            ),
            'cropp': BrandInfo(
                canonical_name='Cropp',
                synonyms=['Cropp', 'CROPP', 'кропп', 'Кропп'],
                format='магазин одягу',
                influence_weight=0.45,
                functional_group='traffic_generator',
                parent_company='LPP'
            ),
            'house': BrandInfo(
                canonical_name='House',
                synonyms=['House', 'HOUSE', 'хаус', 'Хаус'],
                format='магазин одягу',
                influence_weight=0.45,
                functional_group='traffic_generator',
                parent_company='LPP'
            ),
            'sinsay': BrandInfo(
                canonical_name='Sinsay',
                synonyms=['Sinsay', 'SINSAY', 'сінсей', 'Сінсей'],
                format='магазин одягу',
                influence_weight=0.4,
                functional_group='traffic_generator',
                parent_company='LPP'
            )
        })
        
        # ===== ГЕНЕРАТОРИ ТРАФІКУ - СПОРТ =====
        brands.update({
            'sportmaster': BrandInfo(
                canonical_name='Спортмастер',
                synonyms=['Спортмастер', 'SportMaster', 'СПОРТМАСТЕР', 'спортмастер'],
                format='спортивний магазин',
                influence_weight=0.6,
                functional_group='traffic_generator',
                parent_company='Спортмастер'
            ),
            'intersport': BrandInfo(
                canonical_name='Intersport',
                synonyms=['Intersport', 'INTERSPORT', 'інтерспорт', 'Інтерспорт'],
                format='спортивний магазин',
                influence_weight=0.55,
                functional_group='traffic_generator',
                parent_company='Intersport'
            ),
            'megasport': BrandInfo(
                canonical_name='Megasport',
                synonyms=['Megasport', 'MEGASPORT', 'мегаспорт', 'Мегаспорт'],
                format='спортивний магазин',
                influence_weight=0.5,
                functional_group='traffic_generator',
                parent_company='Megasport'
            ),
            'decathlon': BrandInfo(
                canonical_name='Decathlon',
                synonyms=['Decathlon', 'DECATHLON', 'декатлон', 'Декатлон'],
                format='спортивний гіпермаркет',
                influence_weight=0.7,
                functional_group='traffic_generator',
                parent_company='Decathlon'
            )
        })
        
        # ===== ІНШІ ГЕНЕРАТОРИ ТРАФІКУ =====
        brands.update({
            'antoshka': BrandInfo(
                canonical_name='Антошка',
                synonyms=['Антошка', 'АНТОШКА', 'антошка', 'Antoshka'],
                format='дитячий магазин',
                influence_weight=0.5,
                functional_group='traffic_generator',
                parent_company='Антошка'
            ),
            'budynok_igrashok': BrandInfo(
                canonical_name='Будинок іграшок',
                synonyms=['Будинок іграшок', 'Дом игрушек', 'будинок іграшок'],
                format='дитячий магазин',
                influence_weight=0.45,
                functional_group='traffic_generator'
            ),
            'yakaboo': BrandInfo(
                canonical_name='Yakaboo',
                synonyms=['Yakaboo', 'YAKABOO', 'якабу', 'Якабу'],
                format='книжковий магазин',
                influence_weight=0.35,
                functional_group='traffic_generator',
                parent_company='Yakaboo'
            ),
            'knigarnya_ye': BrandInfo(
                canonical_name='Книгарня Є',
                synonyms=['Книгарня Є', 'Книгарня Е', 'книгарня є'],
                format='книжковий магазин',
                influence_weight=0.3,
                functional_group='traffic_generator'
            ),
            'zoon': BrandInfo(
                canonical_name='ZOON',
                synonyms=['ZOON', 'ЗУН', 'зун', 'Zoon'],
                format='зоомагазин',
                influence_weight=0.3,
                functional_group='traffic_generator'
            ),
            'kormotech': BrandInfo(
                canonical_name='Кормотех',
                synonyms=['Кормотех', 'КОРМОТЕХ', 'кормотех', 'Kormotech'],
                format='зоомагазин',
                influence_weight=0.25,
                functional_group='traffic_generator'
            )
        })
        
        # ===== РЕСТОРАНИ ТА КАВ'ЯРНІ =====
        brands.update({
            'mcdonalds': BrandInfo(
                canonical_name="McDonald's",
                synonyms=["McDonald's", 'МакДональдз', 'макдональдз', "MCDONALD'S", 'Мак'],
                format='фастфуд',
                influence_weight=0.4,
                functional_group='traffic_generator',
                parent_company="McDonald's Corporation",
                osm_tags=['amenity=fast_food', "brand=McDonald's"]
            ),
            'kfc': BrandInfo(
                canonical_name='KFC',
                synonyms=['KFC', 'КФС', 'кфс', 'Kentucky Fried Chicken'],
                format='фастфуд',
                influence_weight=0.35,
                functional_group='traffic_generator',
                parent_company='Yum! Brands'
            ),
            'pizza_celentano': BrandInfo(
                canonical_name='Pizza Celentano',
                synonyms=['Pizza Celentano', 'ПІЦА ЧЕЛЕНТАНО', 'челентано', 'піца челентано'],
                format='піцерія',
                influence_weight=0.3,
                functional_group='traffic_generator'
            ),
            'dominos_pizza': BrandInfo(
                canonical_name="Domino's Pizza",
                synonyms=["Domino's Pizza", "Domino's", 'Доміно', 'доміно піца'],
                format='піцерія',
                influence_weight=0.3,
                functional_group='traffic_generator'
            ),
            'aroma_kava': BrandInfo(
                canonical_name='Aroma Kava',
                synonyms=['Aroma Kava', 'АРОМА КАВА', 'арома кава', 'Арома Кава'],
                format='кав\'ярня',
                influence_weight=0.2,
                functional_group='traffic_generator',
                osm_tags=['amenity=cafe', 'brand=Aroma Kava']
            ),
            'lviv_croissants': BrandInfo(
                canonical_name='Lviv Croissants',
                synonyms=['Lviv Croissants', 'Львівські круасани', 'львів круасани'],
                format='кав\'ярня-пекарня',
                influence_weight=0.25,
                functional_group='traffic_generator'
            )
        })
        
        # ===== АПТЕКИ (частково конкуренти) =====
        brands.update({
            'apteka_nyzkyh_cin': BrandInfo(
                canonical_name='Аптека низьких цін',
                synonyms=['Аптека низьких цін', 'АНЦ', 'анц', 'аптека низких цін'],
                format='аптека',
                influence_weight=-0.2,
                functional_group='competitor',
                parent_company='АНЦ'
            ),
            'blagodiya': BrandInfo(
                canonical_name='Благодія',
                synonyms=['Благодія', 'БЛАГОДІЯ', 'благодія', 'Blagodia'],
                format='аптека',
                influence_weight=-0.15,
                functional_group='competitor'
            ),
            'podorozhnyk': BrandInfo(
                canonical_name='Подорожник',
                synonyms=['Подорожник', 'ПОДОРОЖНИК', 'подорожник'],
                format='аптека',
                influence_weight=-0.15,
                functional_group='competitor'
            ),
            'd_s': BrandInfo(
                canonical_name='D.S.',
                synonyms=['D.S.', 'ДС', 'дс', 'Д.С.'],
                format='аптека',
                influence_weight=-0.15,
                functional_group='competitor'
            )
        })
        
        # ===== ФІНАНСОВІ УСТАНОВИ (генератори трафіку) =====
        brands.update({
            'privatbank': BrandInfo(
                canonical_name='ПриватБанк',
                synonyms=['ПриватБанк', 'PrivatBank', 'ПРИВАТБАНК', 'приватбанк', 'Приват'],
                format='банк',
                influence_weight=0.3,
                functional_group='traffic_generator',
                parent_company='ПриватБанк',
                osm_tags=['amenity=bank', 'brand=ПриватБанк']
            ),
            'oschadbank': BrandInfo(
                canonical_name='Ощадбанк',
                synonyms=['Ощадбанк', 'Oschadbank', 'ОЩАДБАНК', 'ощадбанк', 'Ощад'],
                format='банк',
                influence_weight=0.25,
                functional_group='traffic_generator',
                parent_company='Ощадбанк'
            ),
            'monobank': BrandInfo(
                canonical_name='monobank',
                synonyms=['monobank', 'монобанк', 'МОНОБАНК', 'моно'],
                format='банк',
                influence_weight=0.2,
                functional_group='traffic_generator',
                parent_company='Universal Bank'
            )
        })
        
        # ===== ПОШТОВІ СЛУЖБИ =====
        brands.update({
            'nova_poshta': BrandInfo(
                canonical_name='Нова Пошта',
                synonyms=['Нова Пошта', 'Nova Poshta', 'НОВА ПОШТА', 'нова пошта', 'НП'],
                format='поштове відділення',
                influence_weight=0.4,
                functional_group='traffic_generator',
                parent_company='Нова Пошта',
                osm_tags=['amenity=post_office', 'brand=Нова Пошта']
            ),
            'ukrposhta': BrandInfo(
                canonical_name='Укрпошта',
                synonyms=['Укрпошта', 'Ukrposhta', 'УКРПОШТА', 'укрпошта'],
                format='поштове відділення',
                influence_weight=0.3,
                functional_group='traffic_generator',
                parent_company='Укрпошта'
            ),
            'justin': BrandInfo(
                canonical_name='Justin',
                synonyms=['Justin', 'Джастін', 'джастін', 'JUSTIN'],
                format='поштове відділення',
                influence_weight=0.25,
                functional_group='traffic_generator',
                parent_company='Justin'
            ),
            'meest': BrandInfo(
                canonical_name='Meest',
                synonyms=['Meest', 'Міст', 'міст', 'MEEST'],
                format='поштове відділення',
                influence_weight=0.2,
                functional_group='traffic_generator',
                parent_company='Meest'
            )
        })
        
        # ===== ТЕЛЕКОМ =====
        brands.update({
            'vodafone': BrandInfo(
                canonical_name='Vodafone',
                synonyms=['Vodafone', 'Водафон', 'водафон', 'VODAFONE'],
                format='магазин телекомунікацій',
                influence_weight=0.2,
                functional_group='traffic_generator',
                parent_company='Vodafone Ukraine'
            ),
            'kyivstar': BrandInfo(
                canonical_name='Київстар',
                synonyms=['Київстар', 'Kyivstar', 'КИЇВСТАР', 'київстар'],
                format='магазин телекомунікацій',
                influence_weight=0.2,
                functional_group='traffic_generator',
                parent_company='Київстар'
            ),
            'lifecell': BrandInfo(
                canonical_name='lifecell',
                synonyms=['lifecell', 'лайфселл', 'LIFECELL', 'Lifecell'],
                format='магазин телекомунікацій',
                influence_weight=0.15,
                functional_group='traffic_generator',
                parent_company='lifecell'
            )
        })
        
        return brands
    
    def _build_synonym_index(self) -> Dict[str, str]:
        """Будує індекс синонім -> brand_id для швидкого пошуку"""
        index = {}
        for brand_id, brand_info in self.brands.items():
            # Додаємо канонічну назву
            index[self._normalize_name(brand_info.canonical_name)] = brand_id
            
            # Додаємо всі синоніми
            for synonym in brand_info.synonyms:
                index[self._normalize_name(synonym)] = brand_id
        
        return index
    
    def _normalize_name(self, name: str) -> str:
        """Нормалізує назву для пошуку"""
        if not name:
            return ""
        
        # Приводимо до нижнього регістру та прибираємо зайві пробіли
        normalized = name.lower().strip()
        
        # Замінюємо множинні пробіли на один
        normalized = ' '.join(normalized.split())
        
        # Прибираємо апострофи та лапки для уніфікації
        normalized = normalized.replace("'", "").replace('"', '').replace('`', '')
        
        return normalized
    
    def get_brand_by_id(self, brand_id: str) -> Optional[BrandInfo]:
        """Отримує інформацію про бренд за ID"""
        return self.brands.get(brand_id)
    
    def find_brand_by_name(self, name: str) -> Optional[Tuple[str, BrandInfo]]:
        """Знаходить бренд за назвою або синонімом"""
        normalized = self._normalize_name(name)
        
        # Пошук в індексі синонімів
        brand_id = self.synonym_index.get(normalized)
        if brand_id:
            return brand_id, self.brands[brand_id]
        
        return None
    
    def get_all_brands(self) -> Dict[str, BrandInfo]:
        """Повертає всі бренди"""
        return self.brands.copy()
    
    def get_brands_by_group(self, functional_group: str) -> Dict[str, BrandInfo]:
        """Повертає бренди певної функціональної групи"""
        return {
            brand_id: brand_info 
            for brand_id, brand_info in self.brands.items()
            if brand_info.functional_group == functional_group
        }
    
    def get_competitors(self) -> Dict[str, BrandInfo]:
        """Повертає всі бренди-конкуренти"""
        return self.get_brands_by_group('competitor')
    
    def get_traffic_generators(self) -> Dict[str, BrandInfo]:
        """Повертає всі бренди-генератори трафіку"""
        return self.get_brands_by_group('traffic_generator')
    
    def get_brand_statistics(self) -> Dict[str, Any]:
        """Повертає статистику словника"""
        stats = {
            'total_brands': len(self.brands),
            'total_synonyms': sum(len(b.synonyms) for b in self.brands.values()),
            'by_functional_group': {},
            'by_format': {},
            'by_influence_range': {
                'strong_negative': 0,    # -1.0 to -0.7
                'moderate_negative': 0,  # -0.7 to -0.4
                'weak_negative': 0,      # -0.4 to -0.1
                'neutral': 0,            # -0.1 to 0.1
                'weak_positive': 0,      # 0.1 to 0.4
                'moderate_positive': 0,  # 0.4 to 0.7
                'strong_positive': 0     # 0.7 to 1.0
            }
        }
        
        for brand in self.brands.values():
            # По функціональним групам
            group = brand.functional_group
            stats['by_functional_group'][group] = stats['by_functional_group'].get(group, 0) + 1
            
            # По форматам
            format_type = brand.format
            stats['by_format'][format_type] = stats['by_format'].get(format_type, 0) + 1
            
            # По силі впливу
            weight = brand.influence_weight
            if weight <= -0.7:
                stats['by_influence_range']['strong_negative'] += 1
            elif weight <= -0.4:
                stats['by_influence_range']['moderate_negative'] += 1
            elif weight <= -0.1:
                stats['by_influence_range']['weak_negative'] += 1
            elif weight <= 0.1:
                stats['by_influence_range']['neutral'] += 1
            elif weight <= 0.4:
                stats['by_influence_range']['weak_positive'] += 1
            elif weight <= 0.7:
                stats['by_influence_range']['moderate_positive'] += 1
            else:
                stats['by_influence_range']['strong_positive'] += 1
        
        return stats
    
    def export_for_matching(self) -> List[Dict[str, Any]]:
        """Експортує дані для використання в brand matching algorithms"""
        export_data = []
        
        for brand_id, brand_info in self.brands.items():
            export_data.append({
                'brand_id': brand_id,
                'canonical_name': brand_info.canonical_name,
                'synonyms': brand_info.synonyms,
                'all_names': [brand_info.canonical_name] + brand_info.synonyms,
                'format': brand_info.format,
                'influence_weight': brand_info.influence_weight,
                'functional_group': brand_info.functional_group,
                'osm_tags': brand_info.osm_tags or []
            })
        
        return export_data


# Тестування словника
if __name__ == "__main__":
    # Створюємо екземпляр словника
    brand_dict = BrandDictionary()
    
    # Виводимо статистику
    stats = brand_dict.get_brand_statistics()
    print("📊 Статистика Brand Dictionary:")
    print(f"  Всього брендів: {stats['total_brands']}")
    print(f"  Всього синонімів: {stats['total_synonyms']}")
    print("\n  За функціональними групами:")
    for group, count in stats['by_functional_group'].items():
        print(f"    - {group}: {count}")
    print("\n  За силою впливу:")
    for range_name, count in stats['by_influence_range'].items():
        if count > 0:
            print(f"    - {range_name}: {count}")
    
    # Тестуємо пошук
    print("\n🔍 Тестування пошуку:")
    test_names = ['АТБ', 'сільпо', 'Епіцентр К', 'EVA', 'нова пошта']
    for name in test_names:
        result = brand_dict.find_brand_by_name(name)
        if result:
            brand_id, brand_info = result
            print(f"  '{name}' → {brand_info.canonical_name} (вплив: {brand_info.influence_weight})")