import csv
import json
import os
import statistics
from dateutil import parser
from codecs import encode, decode


csv.field_size_limit(2147483647)


def decode_row(string_row):
    result = decode(encode(string_row.strip(), 'latin-1', 'backslashreplace'), 'unicode-escape')
    result1 = result.replace("\"\"\"", "")
    result = result1.replace("\"\"", "\"")
    result1 = result.replace("\"{", "{")
    result = result1.replace("}\"", "}")
    result1 = " ".join(line.strip() for line in result.splitlines())
    return result1


def csv_parse():
    with open('csv_data/detection_18.csv', "r", newline="", encoding="utf-8") as file:
        reader = csv.reader(file)
        data_list = []
        for row in reader:
            result_csv_row = decode_row(row[21])
            try:
                data_json = json.loads(result_csv_row)
                data_list.append(data_json)
            except:
                pass

        with open('data_file.json', 'w') as f:
            json.dump(data_list, f)
        f.close()


def zip_with_gaps(*iterables):
    min_len = min(len(x) for x in iterables)
    result = []
    for i in range(min_len):
        tuple = ()
        for x in iterables:
            tuple += (x[i],)
        result.append(tuple)
    for i in range(min_len, max(len(x) for x in iterables)):
        tuple = ()
        for x in iterables:
            if i < len(x):
                tuple += (x[i],)
            else:
                tuple += ('-',)
        result.append(tuple)
    return result


def get_path_and_file():
    file_list = []
    json_files = []
    folder_path = 'test_data'      # Это корневая папка с файлами
    file_list = os.listdir(folder_path)
    for file in file_list:
        if file.endswith('.json'):
            file_path = os.path.join(folder_path, file)
            json_files.append(file_path)
        else:
            continue
    return json_files


def receiving_csv(grz, zone_names, detection_zones, start_times, end_times, traffic_light_states, traffic_light_start_times, filename):
    with open(f'{filename}_TEST.csv', mode='w', encoding='utf-16', newline='') as file:
        writer = csv.writer(file, delimiter='\t')  # , delimiter=','

        writer.writerow(
            (
                'ГРЗ',
                'Зоны',
                'uuid зоны',
                'Время входа в зону',
                'Время выхода из зоны',
                'Состояние светофора',
                'Время светофора',
                # 'Начало пути',
                # 'Конец пути',
            )
        )
    with open(f'{filename}_TEST.csv', mode='a', encoding='utf-16', newline='') as file:
        writer = csv.writer(file, delimiter='\t')  # , delimiter=','

        for z_name, z_uuid, s_time, e_time, tl_state, tl_time in zip_with_gaps(
                zone_names, detection_zones, start_times, end_times, traffic_light_states, traffic_light_start_times):

        # for z_name, z_uuid, s_time, e_time, tl_state, tl_time in zip_longest(
        #         zone_names, detection_zones, start_times, end_times, traffic_light_states, traffic_light_start_times,
        #         fillvalue='-'):
        # записываем каждый элемент в соответствующий столбец
            writer.writerow(
                (
                    grz,
                    z_name,
                    z_uuid,
                    s_time,
                    e_time,
                    tl_state,
                    tl_time,
                    # profile_start,
                    # profile_end,
                )
            )


def determination_of_average_speed(file_gps):
    with open(file_gps, 'r', encoding='utf-8') as file:
        gps_track = json.load(file)['result']
    for file in get_path_and_file():
        with open(file, 'r', encoding='utf-8') as f:
            detection = json.load(f)
        grz = detection['meta']['profile']['lp']
        start_times = []
        end_times = []
        detection_zones_movement = []

        start_times.append(parser.parse(detection['meta']['profile']['profileStartedTimestamp']).replace(tzinfo=None))
        end_times.append(parser.parse(detection['meta']['profile']['profileFinishedTimestamp']).replace(tzinfo=None))

        # TODO Вывод максимума и минимума.
        speeds = []
        for start, end in zip(start_times, end_times):
            for point in gps_track:
                point_time = parser.parse(point['date'] + ' ' + point['time']).replace(tzinfo=None)
                if start <= point_time <= end:
                    speeds.append(point['Speed,km/h'])
        try:
            if speeds:
                average_speed = statistics.mean(speeds)
                max_speed = max(speeds)
                min_speed = min(speeds)
                del_speed = max_speed - min_speed
                gps_delta = round(1 - abs(min_speed / max_speed), 3) * 100
                # Выводим время входа и выхода в формате "%H:%M:%S"

                # print(f'min\t{start_times[0].strftime("%H:%M:%S")}\t{min_speed:.2f}')  # TODO минимальная скорость
                print(f'ave\t{start_times[0].strftime("%H:%M:%S")}\t{average_speed:.2f}')  # TODO СРЕДНЯЯ СКОРОСТЬ
                # print(f'max\t{start_times[0].strftime("%H:%M:%S")}\t{max_speed:.2f}')  # TODO максимальная скорость
                # print(f'del_speed\t{start_times[0].strftime("%H:%M:%S")}\t{gps_delta:.2f}')  #TODO вывод дельты скорости
                # print(f'max\t{start_times[0].strftime("%H:%M:%S")}\t{round(max_speed, 2)}\tmin\t{round(min_speed, 2)}')  # TODO минимума и максимума


                # print(f'Время выхода: {end_times[-1].strftime("%H:%M:%S")}')

                # print(f'Средняя скорость за периоды времени: {average_speed:.2f} км/ч')
                # Очищаем список скоростей для следующего периода
                speeds.clear()
            else:
                average_speed = statistics.mean(speeds)
                print(f'{start_times[0].strftime("%H:%M:%S")}\t Нет данных о скорости за периоды времени')
        except:
            print(f'{start_times[0].strftime("%H:%M:%S")}\t Нет данных о скорости за периоды времени')


def main():
    file_gps = 'data_for_gps_handler/ID496_2024-02-10-tracking.json'  # TODO ИМЯ ТРЕКЕРА
    determination_of_average_speed(file_gps)


if __name__ == '__main__':
    main()


