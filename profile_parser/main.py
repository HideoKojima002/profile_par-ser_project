import csv
import json
import datetime
import os
import re
import pandas as pd
# import matplotlib.dates as mdates
from dateutil import parser
from itertools import zip_longest
from codecs import encode, decode


csv.field_size_limit(2147483647)
cur_time = datetime.datetime.now().strftime('%d_%m_%Y_%H_%M')


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


def decode_row(string_row):
    result = decode(encode(string_row.strip(), 'latin-1', 'backslashreplace'), 'unicode-escape')
    result1 = result.replace("\"\"\"", "")
    result = result1.replace("\"\"", "\"")
    result1 = result.replace("\"{", "{")
    result = result1.replace("}\"", "}")
    result1 = " ".join(line.strip() for line in result.splitlines())
    return result1


def csv_converter(csv_filename):
    with open(f'test_data/{csv_filename}', "r", newline="", encoding="utf-8") as file:
        reader = csv.reader(file)
        data_list = []
        for row in reader:
            result_csv_row = decode_row(row[21])
            try:
                data_json = json.loads(result_csv_row)
                data_list.append(data_json)
            except:
                pass  # TODO Обработчик исключений
    json_filename = csv_filename.replace('.csv', '') + '.json'
    with open(f'test_data/{cur_time}_{json_filename}', 'w') as f:
        json.dump(data_list, f)
    file.close()
    f.close()
    return f.name

    # with open('data.json', 'w') as file:
    #     json_data = json.dumps(data, indent=4, sort_keys=True)
    #     file.write(json_data)
    #     file.close()
    #
    # print('Новый json файл создан')


def round_time(time):  # функция для округления времени до трех знаков после запятой
    s = time.isoformat()
    s = s[:-3]
    # s = re.sub(r'.*T', '', s) # Убрать дату оставив время
    return s.replace('T', '_')
    # return s


def get_path_and_file():
    file_list = []
    json_files = []
    folder_path = 'test_data'      # Это корневая папка с файлами
    file_list = os.listdir(folder_path)
    for file in file_list:
        if file.endswith('.json'):
            file_path = os.path.join(folder_path, file)
            json_files.append(file_path)
            # collect_data_detection()
        elif file.endswith('.csv'):
            print(file)  # TODO Убарть +
            file = csv_converter(file)
            file_path = os.path.join(file)
            json_files.append(file_path)
        else:
            continue
    print(json_files)
    return json_files


def receiving_csv(grz, zone_names, detection_zones, start_times, end_times, traffic_light_states, traffic_light_start_times, filename):
    with open(f'result/{filename.replace("test_data", "").replace(".json", "")}_{cur_time}.csv', mode='w', encoding='utf-16', newline='') as file:  # {filename}_{grz}_{cur_time}
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
    with open(f'result/{filename.replace("test_data", "").replace(".json", "")}_{cur_time}.csv', mode='a', encoding='utf-16', newline='') as file:
        writer = csv.writer(file, delimiter='\t')  # , delimiter=','

        for z_name, z_uuid, s_time, e_time, tl_state, tl_time in zip_longest(
                zone_names, detection_zones, start_times, end_times, traffic_light_states, traffic_light_start_times,
                fillvalue='-'):
        # записываем каждый элемент в соответствующий столбец
            writer.writerow(
                (
                    grz,
                    z_name,
                    z_uuid,
                    s_time,
                    e_time,
                    # round_time(s_time),
                    # round_time(e_time),
                    tl_state,
                    tl_time,
                    # profile_start,
                    # profile_end,
                )
            )


def collect_data_other_json_file(file):
    with open(file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    profiles_starts = set()
    profiles_finish = set()
    grz = []
    traffic_light_uuids = []
    traffic_light_states = []
    traffic_light_start_times = []
    detection_zones_movement = []
    start_times = []
    end_times = []
    zone_names = []
    zone_dict = {}

    for detection in data:

        grz = detection['meta']['profile']['lp']

        start = parser.parse(detection['meta']['profile']['profileStartedTimestamp']).replace(tzinfo=None)
        end = parser.parse(detection['meta']['profile']['profileFinishedTimestamp']).replace(tzinfo=None)
        if start not in profiles_starts and end not in profiles_finish:
            profiles_starts.add(start)
            profiles_finish.add(end)
        else:
            continue

        for tls in detection['meta']['profile']['trafficLightsStates']:
            traffic_light_uuids.append(tls['uuid'])
            for stated in tls['states']:
                traffic_light_start_times.append(parser.parse(stated['startTimestamp']).replace(tzinfo=None))
                traffic_light_states.append(stated['state'])
                # traffic_light_start_times.append(parser.parse(stated['startTimestamp']).replace(tzinfo=None))
                # light_state = stated['state']
                # if light_state:  # TODO проверка +
                #
                # else:
                #     traffic_light_states.append('-')

        for movement in detection['meta']['profile']['movements']:
            abba = movement['detectionZone']
            detection_zones_movement.append(movement['detectionZone'])

            start_times.append(parser.parse(movement['startTimestamp']).replace(tzinfo=None))
            end_times.append(parser.parse(movement['endTimestamp']).replace(tzinfo=None))

        for zone in detection_zones_movement:
            for z in detection['meta']['profile']['meta']['zones']['zones']:
                if z['uuid'] == zone:
                    zone_dict[zone] = z['name']
                    break

        zone_names = [zone_dict[zone] for zone in detection_zones_movement]
        # for zone in detection_zones_movement:
        #     zone_names.append(zone_dict[zone])
    print(len(grz), len(zone_names), len(detection_zones_movement), len(start_times), len(end_times),
          len(traffic_light_states), len(traffic_light_start_times))
    receiving_csv(grz, zone_names, detection_zones_movement, start_times, end_times, traffic_light_states,
                traffic_light_start_times, f.name.replace('.json', ''))


def collect_data_violation(file):
    with open(file, 'r', encoding='utf-8') as f:
        detection = json.load(f)
    try:
        grz = detection['profile']['lp']

        profile_start = parser.parse(detection['profile']['profileStartedTimestamp']).replace(tzinfo=None)
        profile_end = parser.parse(detection['profile']['profileFinishedTimestamp']).replace(tzinfo=None)

        traffic_light_uuids = []
        traffic_light_states = []
        traffic_light_start_times = []

        for tls in detection['profile']['trafficLightsStates']:
            traffic_light_uuids.append(tls['uuid'])
            for state in tls['states']:
                traffic_light_states.append(state['state'])
                traffic_light_start_times.append(parser.parse(state['startTimestamp']).replace(tzinfo=None))

        detection_zones = []
        start_times = []
        end_times = []

        for movement in detection['profile']['movements']:
            detection_zones.append(movement['detectionZone'])
            start_times.append(parser.parse(movement['startTimestamp']).replace(tzinfo=None))
            end_times.append(parser.parse(movement['endTimestamp']).replace(tzinfo=None))

        zone_dict = {}

        for zone in detection['profile']['meta']['zones']['zones']:
            zone_dict[zone['uuid']] = zone['name']

        zone_names = []

        for zone in detection_zones:
            zone_names.append(zone_dict[zone])

        receiving_csv(grz, zone_names, detection_zones, start_times, end_times, traffic_light_states,
                      traffic_light_start_times, file)
    except:  # KeyError TODO Оброботать ошибку +
        collect_data_other_json_file(file)


def collect_data_detection():
    for file in get_path_and_file():
        with open(file, 'r', encoding='utf-8') as f:
            detection = json.load(f)
        # with open('violation_classifier.json', 'r', encoding='utf-8') as f:
        #     violation = json.load(f)
        try:
            grz = detection['meta']['profile']['lp']

            profile_start = parser.parse(detection['meta']['profile']['profileStartedTimestamp']).replace(tzinfo=None)
            profile_end = parser.parse(detection['meta']['profile']['profileFinishedTimestamp']).replace(tzinfo=None)

            traffic_light_uuids = []
            traffic_light_states = []
            traffic_light_start_times = []

            for tls in detection['meta']['profile']['trafficLightsStates']:
                traffic_light_uuids.append(tls['uuid'])
                for state in tls['states']:
                    traffic_light_states.append(state['state'])
                    traffic_light_start_times.append(parser.parse(state['startTimestamp']).replace(tzinfo=None))

            detection_zones = []
            start_times = []
            end_times = []

            for movement in detection['meta']['profile']['movements']:
                detection_zones.append(movement['detectionZone'])
                start_times.append(parser.parse(movement['startTimestamp']).replace(tzinfo=None))
                end_times.append(parser.parse(movement['endTimestamp']).replace(tzinfo=None))

            zone_dict = {}

            for zone in detection['meta']['profile']['meta']['zones']['zones']:
                zone_dict[zone['uuid']] = zone['name']

            zone_names = []

            for zone in detection_zones:
                zone_names.append(zone_dict[zone])

            receiving_csv(grz, zone_names, detection_zones, start_times, end_times, traffic_light_states,
                          traffic_light_start_times, file)
        except:  # KeyError TODO Обработать ошибку если она будет +
            collect_data_violation(file)


def main():
    collect_data_detection()
    # get_path_and_file()
    # csv_converter()


if __name__ == '__main__':
    main()
