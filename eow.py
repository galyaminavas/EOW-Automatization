import datetime
import os
import sys
import requests
import json
from links_data_eow import amplitude_server, abbreviations, \
    all_charts, is_server, is_projects, platforms, applovin_server, applovin_projects


def get_project_data(project_name, api, secret):
    charts = all_charts[project_name]
    for key in charts:
        print('Getting {} {}... '.format(project_name, key))
        response = requests.get(amplitude_server + charts[key] + '/query',
                                auth=(api, secret))
        print('Response result: {}'.format(response.status_code))
        if response.status_code != 200:
            print('Data is not downloaded')
            return
        else:
            filename = '{}_EOW_{}'.format(project_name, key)
            with open(filename + '.json', 'wb') as code:
                code.write(response.content)
            print('JSON data is downloaded')
            print()
    return


def greet_and_get_project():
    projects = ', '.join(all_charts.keys())
    print('This version of EOW script can get the following projects data: {}.'.format(projects))
    print('Choose the project:')
    project_name = input('> ')
    if project_name not in abbreviations.keys():
        return 'error'
    return project_name


def read_credentials(project_name):
    with open('credentials_eow.json', 'r') as read_file:
        data = json.load(read_file)
    amplitude_api_key = data['amplitude_data'][project_name]['api_key']
    amplitude_secret_key = data['amplitude_data'][project_name]['secret_key']
    iron_source_user = data['iron_source_data']['user_name']
    iron_source_secret_key = data['iron_source_data']['secret_key']
    applovin_key = data['applovin_data']['api_key']
    return amplitude_api_key, amplitude_secret_key, iron_source_user, iron_source_secret_key, applovin_key


def verify_amplitude(project_name, api_key, secret_key):
    test_chart = all_charts[project_name]['installs']
    print('Trying to connect to {} Amplitude with provided credentials...'.format(project_name))
    response = requests.get(amplitude_server + test_chart + '/query',
                            auth=(api_key, secret_key))
    return response.status_code, response.reason


def verify_is(user_name, secret_key):
    date_start = (datetime.datetime.today() - datetime.timedelta(days=8)).strftime('%Y-%m-%d')
    date_end = (datetime.datetime.today() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')
    print('Trying to connect to IronSource with provided credentials...')
    response = requests.get(is_server + 'startDate=' + date_start + '&endDate=' + date_end,
                            auth=(user_name, secret_key))
    return response.status_code, response.reason


def verify_applovin(key):
    date_start = (datetime.datetime.today() - datetime.timedelta(days=8)).strftime('%Y-%m-%d')
    date_end = (datetime.datetime.today() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')
    print('Trying to connect to AppLovin with provided credentials...')
    response = requests.get(applovin_server
                            + '?start=' + date_start
                            + '&end=' + date_end
                            + '&api_key=' + key
                            + '&format=json'
                            + '&columns=application,estimated_revenue')
    return response.status_code, response.reason


def get_ads_data(project_name, user, secret):
    date_start = (datetime.datetime.today() - datetime.timedelta(days=8)).strftime('%Y-%m-%d')
    date_end = (datetime.datetime.today() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')
    print('Getting IS data...')
    response = requests.get(is_server + 'startDate=' + date_start + '&endDate=' + date_end +
                            '&metrics=revenue&breakdown=app',
                            auth=(user, secret))
    print('Response result: {}'.format(response.status_code))
    if response.status_code != 200:
        print('Data is not downloaded')
        return
    else:
        filename = '{}_EOW_IS'.format(project_name)
        with open(filename + '.json', 'wb') as code:
            code.write(response.content)
        print('JSON data is downloaded')
        print()
    return


def get_applovin_data(project_name, key):
    date_start = (datetime.datetime.today() - datetime.timedelta(days=8)).strftime('%Y-%m-%d')
    date_end = (datetime.datetime.today() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')
    print('Getting AppLovin data...')
    response = requests.get(applovin_server
                            + '?start=' + date_start
                            + '&end=' + date_end
                            + '&api_key=' + key
                            + '&format=json'
                            + '&columns=application,estimated_revenue')
    print('Response result: {}'.format(response.status_code))
    if response.status_code != 200:
        print('Data is not downloaded')
        return
    else:
        filename = '{}_EOW_AL'.format(project_name)
        with open(filename + '.json', 'wb') as code:
            code.write(response.content)
        print('JSON data is downloaded')
    return


def parse_installs(project_name):
    with open('{}_EOW_installs.json'.format(project_name), 'r') as read_file:
        data = json.load(read_file)
    analyzed_date_from = data['data']['xValues'][0]
    analyzed_date_to = data['data']['xValues'][6]

    installs_values = []
    for i in range(0, len(platforms[project_name]) + 1):
        installs_values.append(data['data']['seriesCollapsed'][i][0]['value'])

    installs = {'Android Organic': installs_values[0],
                'Android Paid': installs_values[1]
                }
    for i in range(1, len(platforms[project_name])):
        installs[platforms[project_name][i]] = installs_values[i+1]

    return installs, analyzed_date_from, analyzed_date_to


def parse_wau(project_name):
    with open('{}_EOW_wau.json'.format(project_name), 'r') as read_file:
        data = json.load(read_file)

    wau = {}
    for i in range(0, len(platforms[project_name])):
        wau[platforms[project_name][i]] = data['data']['seriesCollapsed'][i][0]['value']

    return wau


def parse_avg_dau(project_name):
    with open('{}_EOW_avg_dau.json'.format(project_name), 'r') as read_file:
        data = json.load(read_file)

    avg_dau_values = []
    for i in range(0, len(platforms[project_name])):
        avg_dau_values.append(0)

    for i in range(0, 7):
        for j in range(0, len(avg_dau_values)):
            avg_dau_values[j] += data['data']['series'][j][i]['value']

    avg_dau = {}
    for i in range(0, len(platforms[project_name])):
        avg_dau[platforms[project_name][i]] = avg_dau_values[i] / 7

    return avg_dau


def parse_iap_revenue(project_name):
    with open('{}_EOW_iap_revenue.json'.format(project_name), 'r') as read_file:
        data = json.load(read_file)

    iap_revenue = {}
    for i in range(0, len(platforms[project_name])):
        iap_revenue[platforms[project_name][i]] = data['data']['seriesCollapsed'][i][0]['value']

    return iap_revenue


def parse_ads_revenue(project_name):
    with open('{}_EOW_IS.json'.format(project_name), 'r') as read_file:
        data = json.load(read_file)

    ads_revenue = {}
    for i in range(0, len(platforms[project_name])):
        ads_revenue[platforms[project_name][i]] = 0

    for i in data:
        for k, v in is_projects[project_name].items():
            if i['appKey'] == v:
                ads_revenue[k] += i['data'][0]['revenue']

    if project_name in applovin_projects:
        with open('{}_EOW_AL.json'.format(project_name), 'r') as read_file:
            data_al = json.load(read_file)
        for i in data_al['results']:
            for k, v in applovin_projects[project_name].items():
                if v == i['application']:
                    ads_revenue[k] += float(i['estimated_revenue'])

    return ads_revenue


def parse_ret(project_name, day=1):
    with open('{}_EOW_{}d_ret.json'.format(project_name, day), 'r') as read_ret:
        data = json.load(read_ret)

    dates = data['data']['series'][0]['dates']
    ret_1d_day1 = data['data']['series'][0]['datetimes'][6]
    ret_1d_day7 = data['data']['series'][0]['datetimes'][0]
    day1 = datetime.date.strftime(datetime.datetime.strptime(ret_1d_day1, '%Y-%m-%d'), '%d/%m/%y')
    day7 = datetime.date.strftime(datetime.datetime.strptime(ret_1d_day7, '%Y-%m-%d'), '%d/%m/%y')

    retention = {}
    for i in range(0, len(platforms[project_name])):
        retention[platforms[project_name][i]] = {}
        retention[platforms[project_name][i]]['day1'] = day1
        retention[platforms[project_name][i]]['day7'] = day7
        retention[platforms[project_name][i]]['installs'] = data['data']['series'][i]['combined'][2]['outof']
        retention[platforms[project_name][i]]['ret_d1'] = 0

    for d in dates:
        for i in range(0, len(platforms[project_name])):
            if data['data']['series'][i]['values'][d][2]['outof'] > 0:
                retention[platforms[project_name][i]]['ret_d1'] += data['data']['series'][i]['values'][d][2]['count'] \
                                                                   / data['data']['series'][i]['values'][d][2]['outof']

    for i in range(0, len(platforms[project_name])):
        retention[platforms[project_name][i]]['ret_d1'] = retention[platforms[project_name][i]]['ret_d1'] * 100 / 7

    if day > 1:
        for i in range(0, len(platforms[project_name])):
            retention[platforms[project_name][i]]['ret_d7'] = 0

        for d in dates:
            for i in range(0, len(platforms[project_name])):
                if data['data']['series'][i]['values'][d][4]['outof'] > 0:
                    retention[platforms[project_name][i]]['ret_d7'] += data['data']['series'][i]['values'][d][4]['count']\
                                                                       / data['data']['series'][i]['values'][d][4]['outof']

        for i in range(0, len(platforms[project_name])):
            retention[platforms[project_name][i]]['ret_d7'] = retention[platforms[project_name][i]]['ret_d7'] * 100 / 7

        if day > 7:
            for i in range(0, len(platforms[project_name])):
                retention[platforms[project_name][i]]['ret_d30'] = 0

            for d in dates:
                for i in range(0, len(platforms[project_name])):
                    if data['data']['series'][i]['values'][d][6]['outof'] > 0:
                        retention[platforms[project_name][i]]['ret_d30'] += data['data']['series'][i]['values'][d][6]['count']\
                                                                            / data['data']['series'][i]['values'][d][6]['outof']

            for i in range(0, len(platforms[project_name])):
                retention[platforms[project_name][i]]['ret_d30'] = retention[platforms[project_name][i]]['ret_d30'] * 100 / 7

            if day > 30:
                for i in range(0, len(platforms[project_name])):
                    retention[platforms[project_name][i]]['ret_d90'] = 0

                for d in dates:
                    for i in range(0, len(platforms[project_name])):
                        if data['data']['series'][i]['values'][d][8]['outof'] > 0:
                            retention[platforms[project_name][i]]['ret_d90'] += data['data']['series'][i]['values'][d][8]['count']\
                                                                                / data['data']['series'][i]['values'][d][8]['outof']

                for i in range(0, len(platforms[project_name])):
                    retention[platforms[project_name][i]]['ret_d90'] = retention[platforms[project_name][i]]['ret_d90'] * 100 / 7

    return retention


def remove_secondary_files(project_name):
    charts = all_charts[project_name]
    for key in charts:
        filename = '{}_EOW_{}.json'.format(project_name, key)
        os.remove(filename)
    os.remove('{}_EOW_IS.json'.format(project_name))
    if project_name in applovin_projects:
        os.remove('{}_EOW_AL.json'.format(project_name))
    return


def get_report(project_name, amp_api, amp_secret, is_user, is_secret, al_key):
    get_project_data(project_name, amp_api, amp_secret)
    get_ads_data(project_name, is_user, is_secret)
    if project_name in applovin_projects:
        get_applovin_data(project_name, al_key)

    installs, date_from, date_to = parse_installs(project_name)
    wau = parse_wau(project_name)
    avg_dau = parse_avg_dau(project_name)
    iap_revenue = parse_iap_revenue(project_name)
    revenue_ads = parse_ads_revenue(project_name)

    retention_d1 = parse_ret(project_name, 1)
    retention_d7 = parse_ret(project_name, 7)
    retention_d30 = parse_ret(project_name, 30)
    retention_d90 = parse_ret(project_name, 90)

    revenue_net = {}
    for k, v in iap_revenue.items():
        if k == 'Android' or k == 'iOS' or k == 'Huawei':
            revenue_net[k] = v * 0.85
        elif k == 'MS':
            revenue_net[k] = v * 0.88
        elif k == 'Amazon':
            revenue_net[k] = v * 0.8

    revenue_total = {}
    for k in revenue_net:
        revenue_total[k] = revenue_net[k] + revenue_ads[k]

    original_stdout = sys.stdout
    with open('{}_EOW_{}_{}.txt'.format(project_name, date_from, date_to), 'w') as f:
        sys.stdout = f
        d1 = datetime.date.strftime(datetime.datetime.strptime(date_from, '%Y-%m-%d'), '%d/%m/%y')
        d7 = datetime.date.strftime(datetime.datetime.strptime(date_to, '%Y-%m-%d'), '%d/%m/%y')
        print('Analytics {} - {}'.format(d1, d7))
        print()
        print()
        for platform in platforms[project_name]:
            print('{} {}'.format(abbreviations[project_name], platform))

            if platform == 'Android':
                print('Installs: {} ({} organic + {} paid)'.format(installs['Android Organic']
                                                                   + installs['Android Paid'],
                                                                   installs['Android Organic'],
                                                                   installs['Android Paid']))
            else:
                print('Installs: {}'.format(installs[platform]))

            print('WAU: {}'.format(wau[platform]))
            print('AVG DAU: {:.0f}'.format(avg_dau[platform]))
            print('Revenue: ${:.2f}'.format(revenue_total[platform]))
            print('Revenue IAP: ${:.2f} (${:.2f})'.format(revenue_net[platform], iap_revenue[platform]))
            print('Revenue Ads: ${:.2f}'.format(revenue_ads[platform]))
            print()
            print('Retention:')
            print('{} - {}: {} installs, D1 - {:.2f}%'
                  .format(retention_d1[platform]['day1'], retention_d1[platform]['day7'],
                          retention_d1[platform]['installs'], retention_d1[platform]['ret_d1']))

            print('{} - {}: {} installs, D1 - {:.2f}%, D7 - {:.2f}%'
                  .format(retention_d7[platform]['day1'], retention_d7[platform]['day7'],
                          retention_d7[platform]['installs'], retention_d7[platform]['ret_d1'],
                          retention_d7[platform]['ret_d7']))
            print('{} - {}: {} installs, D1 - {:.2f}%, D7 - {:.2f}%, D30 - {:.2f}%'
                  .format(retention_d30[platform]['day1'], retention_d30[platform]['day7'],
                          retention_d30[platform]['installs'], retention_d30[platform]['ret_d1'],
                          retention_d30[platform]['ret_d7'], retention_d30[platform]['ret_d30']))
            print('{} - {}: {} installs, D1 - {:.2f}%, D7 - {:.2f}%, D30 - {:.2f}%, D90 - {:.2f}%'
                  .format(retention_d90[platform]['day1'], retention_d90[platform]['day7'],
                          retention_d90[platform]['installs'], retention_d90[platform]['ret_d1'],
                          retention_d90[platform]['ret_d7'], retention_d90[platform]['ret_d30'],
                          retention_d90[platform]['ret_d90']))
            print()

    sys.stdout = original_stdout
    print()
    print('EOW analytics is written to the {}_EOW_{}_{}.txt file'.format(project_name, date_from, date_to))

    remove_secondary_files(project_name)
    return


def main():
    project_name = greet_and_get_project()
    if project_name == 'error':
        print('Project name is incorrect.')
        input()
        return

    amp_api, amp_secret, is_user, is_secret, al_key = read_credentials(project_name)

    amp_result, amp_reason = verify_amplitude(project_name, amp_api, amp_secret)
    if amp_result != 200:
        print('Response result: {}'.format(amp_result))
        print('Reason: {}'.format(amp_reason))
        print('Check the provided credentials and try again.')
        input()
        return
    else:
        print('Success!')

    is_result, is_reason = verify_is(is_user, is_secret)
    if is_result != 200:
        print('Response result: {}'.format(is_result))
        print('Reason: {}'.format(is_reason))
        print('Check the provided credentials and try again.')
        input()
        return
    else:
        print('Success!')

    if project_name in applovin_projects:
        al_result, al_reason = verify_applovin(al_key)
        if al_result != 200:
            print('Response result: {}'.format(al_result))
            print('Reason: {}'.format(al_reason))
            print('Check the provided credentials and try again.')
            input()
            return
        else:
            print('Success!')

    get_report(project_name, amp_api, amp_secret, is_user, is_secret, al_key)
    input()


if __name__ == '__main__':
    main()
