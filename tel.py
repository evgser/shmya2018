from datetime import datetime, timedelta
from multiprocessing import Pool
import matplotlib.pyplot as plt
import sys


class ParsingLog:

    def __init__(self):
        self.log_file = ''
        self.title = []
        self.main_keys = []
        self.sub_keys = []
        self.keys_channel_list = []
        self.tv_programm = []
        self.minus_keys = []
        self.uniq_channels = []

    def set_title(self, title):
        self.title = title

    def set_logfile(self, log_file):
        self.log_file = log_file

    def set_main_keys(self, keys):
        self.main_keys = keys

    def set_sub_keys(self, keys):
        self.sub_keys = keys

    def set_keys_channel_list(self, keys):
        for i in range(len(keys)):
            self.keys_channel_list.append(keys[i].split())

    def set_tv_programm(self, keys):
        for i in range(len(keys)):
            self.tv_programm.append(keys[i].split())

    def set_minus_keys(self, keys):
        self.minus_keys = keys

    def set_uniq_channels(self, keys):
        self.uniq_channels = keys

    # Функция чтения фалйа логов
    def open_and_read_file(self):
        log = open(self.log_file, 'r', encoding="utf8")  # открываем файл
        all_line = log.readlines()  # читаем
        log.close()
        return all_line

    # Функция заполненения данных в удобную словарь
    def fill_dict(self, st):

        mid = st.find('http')  # делим пополам

        # Работаем с первой половиной
        main_part = st[:mid].split()  # сохраняем первую половину

        date = list(map(int, main_part[1].split('-')))  # преобразуем дату
        time = list(map(int, main_part[2].split(':')))  # преобразуем время
        main_part.remove(main_part[2])  # удаляем лишнее, чтобы ничего не сдвигалось

        main_dict = {self.title[0]: int(main_part[0]), self.title[1]: datetime(date[0], date[1], date[2], time[0], time[1]),
                     self.title[2]: main_part[2], self.title[3]: int(main_part[3]), self.title[4]: int(main_part[4])}

        # Работаем со второй половиной
        sub_part = st[mid:]  # сохраняем вторую половину
        key_part = sub_part[sub_part.find('=') + 1: sub_part[5:].find('http') + 4]  # ключи запроса и служебная инфа
        url_part = sub_part[sub_part[5:].find('http') + 5 : len(sub_part) - 1]  # url, что открывал

        key_part = key_part.split()  # дробим слова
        if key_part != [] and key_part[len(key_part) - 1].find('&'):  # если запрос не пустой
            sub_service = key_part[len(key_part) - 1].split('&')
            service_part = sub_service[1:]
            key_part[len(key_part) - 1] = sub_service[0]
            main_dict[self.title[5]] = key_part
            main_dict[self.title[6]] = service_part
        else:
            main_dict[self.title[5]] = key_part
            main_dict[self.title[6]] = []

        main_dict[self.title[7]] = url_part.split(';')  # добавляем url

        sub_keys = []
        for i in range(len(main_dict[self.title[5]])):
            sub_keys.append(main_dict[self.title[5]][i].lower())
        main_dict[self.title[5]] = sub_keys

        search_category = self.search_key_words(main_dict[self.title[5]])
        if search_category:
            main_dict['search_category'] = search_category
            return main_dict

    # Функция для проверки запроса на релевантность
    def search_key_words(self, log_words):
        search_category = []
        # Проверка с основными ключами
        for j in range(len(self.main_keys)):
            if self.main_keys[j] in log_words:
                count_main = 0
                for k in range(len(log_words)):
                    if log_words[k] not in self.minus_keys:
                        count_main += 1
                if count_main == len(log_words):
                    search_category.append('m')
                    break

        # Проверка каналы+телешоу
        for j in range(len(self.keys_channel_list)):
            count_mix_1 = 0
            for k in range(len(self.keys_channel_list[j])):
                if self.keys_channel_list[j][k] in log_words:
                    count_mix_1 += 1
                if count_mix_1 == len(self.keys_channel_list[j]):
                    for j2 in range(len(self.tv_programm)):
                        count_mix_2 = 0
                        for k2 in range(len(self.tv_programm[j2])):
                            if self.tv_programm[j2][k2] in log_words:
                                count_mix_2 += 1
                        if count_mix_2 == len(self.tv_programm[j2]):
                            search_category.append('chts')
                            break

        # Проверка по уникальным каналам
        for j in range(len(self.uniq_channels)):
            if self.uniq_channels[j] in log_words:
                search_category.append('ch')
                break

        # Проверка по каналам и телешоу
        for j in range(len(self.sub_keys)):
            if self.sub_keys[j] in log_words:
                for k in range(len(self.keys_channel_list)):
                    count_channel = 0
                    for k2 in range(len(self.keys_channel_list[k])):
                        if self.keys_channel_list[k][k2] in log_words:
                            count_channel += 1
                    if count_channel == len(self.keys_channel_list[k]):
                        search_category.append('ch')
                        break

                for t in range(len(self.tv_programm)):
                    count_programm = 0
                    for t2 in range(len(self.tv_programm[t])):
                        if self.tv_programm[t][t2] in log_words:
                            count_programm += 1
                    if count_programm == len(self.tv_programm[t]):
                        search_category.append('ts')
                        break

        if search_category:
            return search_category
        else:
            return None


if __name__ == '__main__':

    # Функция для чтения файла с ключами для выделения групп в заросах
    def read_keys(name_keys):
        with open(name_keys, 'r', encoding="utf8") as keys:
            lines = keys.read().splitlines()
        return lines

    # Парсим лог файл, возвращаем только целевые запросы и общее количество запросов
    def pars_log():

        xzibit = ParsingLog()  # создаём объект

        xzibit.set_logfile('log')  # устанавливаем название логов в класс
        st = xzibit.open_and_read_file()  # читаем и заносим строки в список

        count_request = len(st) - 1  # количество запросов изначально

        # Получаем заголовки для словаря
        names_log = st[0].split()
        names_log.insert(6, 'service')
        """ timestamp - int
            datetime - datetime
            device - string
            numdoc - int
            region - int
            request - list
            service - list
            url - list
            + search_category - list
            """
        xzibit.set_title(names_log)  # заголовки их в класс

        # Читаем ключи, по которым будем искать запросы
        main_keys = read_keys('main_keys')
        sub_keys = read_keys('sub_keys')
        keys_channel_list = read_keys('keys_channel_list')
        tv_programm = read_keys('tv_programm')
        minus_keys = read_keys('minus_keys')
        uniq_channels = read_keys('keys_uniq_channel_list')

        # Устанавливаем ключи
        xzibit.set_main_keys(main_keys)
        xzibit.set_sub_keys(sub_keys)
        xzibit.set_keys_channel_list(keys_channel_list)
        xzibit.set_tv_programm(tv_programm)
        xzibit.set_minus_keys(minus_keys)
        xzibit.set_uniq_channels(uniq_channels)

        pool = Pool()  # создаём бассейн, в котором будут купаться наши данные
        tv_list = list(pool.map(xzibit.fill_dict, st[1:len(st)]))  # многопоточно и асинхронно обрабатываем данные
        tv_list = list(filter(lambda x: x is not None, tv_list))  # чистим бассейн от мусора

        pool.close()  # закрываем бассейн, он нам больше не нужен
        pool.join()  # возвращаемся в реальный мир

        return tv_list, count_request

    # Функция для рисования диаграммы name с пунктами data_names и данными data_values соответственно
    def draw_diagram(name, data_names, data_values):

        plt.title(name)
        plt.pie(
            data_values, autopct='%1.1f%%', radius=0.8,
            explode=[0.1] + [0 for _ in range(len(data_names) - 1)]
        )
        plt.legend(
            bbox_to_anchor=(0.05, 0.7, 0.25, 0.25),
            loc='best', labels=data_names
        )

        plt.show()

    # Функция для рисования графика распределения запросов по времени
    def draw_time(data_list):

        x = []
        y = [0]
        for i in range(len(data_list)):
            x.append(data_list[i]['datetime'])

        meantime = datetime(year=data_list[0]['datetime'].year, month=data_list[0]['datetime'].month,
                            day=data_list[0]['datetime'].day, hour=0, minute=30)
        i = 0
        j = 0
        xlabels = [meantime.time().isoformat(timespec='minutes')]
        while i < len(data_list):
            if x[i] < meantime:
                y[j] += 1
                i += 1
            else:
                meantime += timedelta(hours=0, minutes=30)
                y.append(0)
                j += 1
                xlabels.append(meantime.time().isoformat(timespec='minutes'))

        ax = plt.axes()
        ax.set_xticklabels(xlabels, rotation=270)
        plt.title('Распределение в течении дня')
        plt.plot(xlabels, y)
        plt.show()

    # Функция для отображения долей запросов по регионам
    def region_data(tv_list):
        x_reg = []
        y_val = []
        for i in range(len(tv_list)):
            if tv_list[i]['region'] in x_reg:
                 y_val[x_reg.index(tv_list[i]['region'])] += 1
            else:
                x_reg.append(tv_list[i]['region'])
                y_val.append(1)

        return [x_reg, y_val]

    # Функция возвращает ТОП регионов по виду запроса
    def region_top(region_list, n):
        x_reg = []
        y_val = []

        for i in range(n):
            top_val = max(region_list[1])
            y_val.append(top_val)
            x_reg.append(region_list[0][region_list[1].index(top_val)])
            region_list[0].remove(region_list[0][region_list[1].index(top_val)])
            region_list[1].remove(top_val)

        return [x_reg, y_val]

    # Функция для вывода статистика name по регионам
    def print_region_top(name, top_reg):
        print(name)
        for i in range(len(top_reg[0])):
            print('Регион %d' % top_reg[0][i], ' : ', top_reg[1][i])

    # Функция для вывода статистики name с пунктами data_names и данными data_values соответственно
    def print_stat(name, data_names, data_values):
        print(name)
        for i in range(len(data_names)):
            print(data_names[i], ' : ', data_values[i])

    # Функция для определения долей пользователей по устройствам
    def desktop_touch(tv_list):
        count_desktop = 0
        count_touch = 0
        for i in range(len(tv_list)):
            if tv_list[i]['device'] == 'desktop':
                count_desktop += 1
            else:
                count_touch += 1
        return [count_desktop, count_touch]

    # Функция возвращает два списка data_names и data_values для выделения запросов по ТВ в общей доле
    def tv_main(count_request, count_tv):
        return 'Запросы по ТВ', ['ТВ', 'Остальные запросы'], [count_tv, count_request - count_tv]

    # Функция возвращает два списка data_names и data_values для выделения запросов по каналам в общей доле
    def tv_sub(tv_list, flag):
        count_tv = 0
        sub_list_tv = []
        for i in range(len(tv_list)):
            if flag in tv_list[i]['search_category']:
                count_tv += 1
                sub_list_tv.append(tv_list[i])
        return sub_list_tv, [count_tv, len(tv_list) - count_tv]

    # Функция поиска по ключевому слову в группе запросов по ТВ
    def tv_find(tv_list, word):
        count_find = 0
        sub_list_tv_find = []
        for i in range(len(tv_list)):
            if word in tv_list[i]['request']:
                count_find += 1
                sub_list_tv_find.append(tv_list[i])
        return sub_list_tv_find, [count_find, len(tv_list) - count_find]

    # Функция обработки дополнительных ключей
    def sub_command(tv_list, name, data_names, data_values):

        if '-r' in sys.argv or '-region' in sys.argv:
            region_list = region_top(region_data(tv_list), 3)
            print_region_top('Топ регионов по запросу', region_list)

        draw_diagram(name, data_names, data_values)

        if '-d' in sys.argv or '-device' in sys.argv:
            name_dev = 'Распределение по платформам'
            data_names_dev = ['desktop', 'touch']
            data_values_dev = desktop_touch(tv_list)
            draw_diagram(name_dev, data_names_dev, data_values_dev)

        if '-t' in sys.argv or '-time' in sys.argv:
            draw_time(tv_list)

    # Разбираем лог файл и возвращаем список целевых заросов и общее количество запросов
    tv_list, count_request = pars_log()
    count_tv = len(tv_list)  # считаем количество целевых запросов

    count_command = len(sys.argv)
    if count_command == 1:
        # Отображаем общую долю телевидения
        name, data_names, data_values = tv_main(count_request, count_tv)  # формируем данные
        print_stat(name, data_names, data_values)  # печатаем статистику
    else:
        # Проверяем на вызов скрипта только с дополнительными ключами
        count_sub = 0
        if '-t' in sys.argv or '-time' in sys.argv:
            count_sub += 1
        if '-d' in sys.argv or '-device' in sys.argv:
            count_sub += 1
        if '-r' in sys.argv or '-region' in sys.argv:
            count_sub += 1

        # Если ключи только дополнительные - обрабатываем основную долю запросов по ТВ
        if count_sub == count_command - 1:
            name, data_names, data_values = tv_main(count_request, count_tv)  # формируем данные
            print_stat(name, data_names, data_values)  # печатаем статистику
            sub_command(tv_list, name, data_names, data_values)  # работаем с дополнительными ключами

        else:
            if '-m' in sys.argv or '-main' in sys.argv:
                name, data_names, data_values = tv_main(count_request, count_tv)  # формируем данные
                print_stat(name, data_names, data_values)  # печатаем статистику
                sub_command(tv_list, name, data_names, data_values)  # работаем с дополнительными ключами

            elif '-ch' in sys.argv or '-channel' in sys.argv:
                # Формируем данные
                sub_tv_list, data_values = tv_sub(tv_list, 'ch')
                name = 'Запросы по телеканалам'
                data_names = ['Телеканалы', 'Остальные запросы по ТВ']
                print_stat(name, data_names, data_values)  # печатаем основную статистику
                sub_command(sub_tv_list, name, data_names, data_values)  # работает с дополнительными ключами

            elif '-ts' in sys.argv or '-tvshow' in sys.argv:
                # Формируем данные
                sub_tv_list, data_values = tv_sub(tv_list, 'ts')
                name = 'Запросы по телешоу'
                data_names = ['Телешоу', 'Остальные запросы по ТВ']
                print_stat(name, data_names, data_values)  # печатаем основную статистику
                sub_command(sub_tv_list, name, data_names, data_values)  # работает с дополнительными ключами

            elif '-chts' in sys.argv or '-channelshow' in sys.argv:
                # Формируем данные
                sub_tv_list, data_values = tv_sub(tv_list, 'chts')
                name = 'Запросы по телеканал+телешоу'
                data_names = ['Канал+телешоу', 'Остальные запросы по ТВ']
                print_stat(name, data_names, data_values)  # печатаем основную статистику
                sub_command(sub_tv_list, name, data_names, data_values)  # работает с дополнительными ключами

            elif '-f' in sys.argv or '-find' in sys.argv:
                try:
                    if '-f' in sys.argv:
                        j = sys.argv.index('-f')
                    else:
                        j = sys.argv.index('-find')

                    word = sys.argv[j + 1].lower()
                    sub_tv_list, data_values = tv_find(tv_list, word)
                    # Формируем данные
                    name = 'Запросы по %s' %word
                    data_names = ['%s' %word, 'Остальные запросы по ТВ']
                    print_stat(name, data_names, data_values)  # печатаем основную статистику
                    sub_command(sub_tv_list, name, data_names, data_values)  # работает с дополнительными ключами

                except Exception:
                    print('Ошибка, воспользуйтесь ключом -help, чтобы получить список команд')

            else:
                # Отображаем список возможныхкоманд
                print('Cписок команд:')
                print(' -h или -help : список команд')
                print(' Категории запросов:')
                print(' -m или -main : запросы по просмотру телевидения')
                print(' -ch или -channel : запросы по телеканалам')
                print(' -ts или -tvshow : запросы по телепередачам')
                print(' -chts или -channelshow : запросы по телеканалу и телепередаче')
                print(' -f word или -find word: запросы по конкретному слову')
                print(' Дополнительная информация по запросам:')
                print(' -t или -time : распределение запросов по времени')
                print(' -d или -device : распределение запросов по платформам')
                print(' -r или -region : распределение запросов по регионам')
