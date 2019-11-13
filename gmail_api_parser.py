from apiclient import discovery
from apiclient import errors
from httplib2 import Http
from oauth2client import file, client, tools
import base64
from bs4 import BeautifulSoup
import csv
from time import strftime, gmtime
import sys

# ПОЧТА ОТПРАВИТЕЛЯ ЗАЯВОК, ТЕБЕ НУЖНО ПОСТАВИТЬ ПОЧТУ ОТ СЕРВИСА С ЗАЯВКАМИ ЗДЕСЬ:
label_q_email_from = 'from:YOU SPECIFIED SENDER EMAIL'
# Вынес эту позицию из функции __name__ сюда, чтоб было видно сразу. Раньше она была под label_id_two на 118 строке

def ReadEmailDetails(service, user_id, msg_id):
  # Аргументы:
  #   service: Авторизация по Gmail API .
  #   user_id: email адрес юзера. Используем значение "me" т.к. это наш адрес
  #   msg_id: ID каждого отдельного сообщения из списка айдишек.

  temp_dict = {}

  try:

      message = service.users().messages().get(userId=user_id, id=msg_id).execute() # Фетчим сообщение по API
      payld = message['payload'] # Получаем payload сообщения
      headr = payld['headers'] # Получаем значения словаря 'header' из словаря 'payload'


      for one in headr: # Получаем Subject письма
          if one['name'] == 'Subject':
              msg_subject = one['value']
              temp_dict['Subject'] = msg_subject
              print(msg_subject)
          else:
              pass


      for two in headr: # Получаем дату
          if two['name'] == 'Date':
              msg_date = two['value']
              print(msg_date)
              temp_dict['DateTime'] = msg_date
          else:
              pass


      # Фетчим тело письма
      email_parts = payld['parts'] # фетчим 'parts' сообщения
      print('\n!!!!...EMAIL_PARTS:')
      print(email_parts)
      part_one  = email_parts[0] # фетчим первый элемент 'parts'
      part_body = part_one['body'] # фетчим 'body' от 'message'
      part_data = part_body['data'] # фетчим 'data' от 'body'
      clean_one = part_data.replace("-","+") # Меняем кодировку из Base64 в UTF-8
      clean_one = clean_one.replace("_","/") # Меняем кодировку из Base64 в UTF-8
      clean_two = base64.b64decode (bytes(clean_one, 'UTF-8')) # Меняем кодировку из Base64 в UTF-8
      soup = BeautifulSoup(clean_two , "lxml" ) # Наводим порядок в выдаче
      message_body = soup.body() # Наводим порядок в выдаче
      print('\n!!!!...MESSAGE BODY:')
      print(message_body)
      # mssg_body это читабельная форма тела письма
      # В зависимости от задачи, можно обработать
      # Через regex, beautiful soup, или другим методом
      temp_dict['Message_body'] = message_body
      print('\n!!!!...TEMP DICT')
      print(temp_dict)

  except Exception as e:
      print(e)
      temp_dict = None
      pass

  finally:
      return temp_dict


def ListMessagesWithLabels(service, user_id, label_ids=[]):
  # Аргументы:
  #   service: Авторизация по Gmail API .
  #   user_id: email адрес юзера. Используем значение "me" т.к. это наш адрес
  #   label_ids: Возвращает сообщения по заданным параметрам.

  try:
    response = service.users().messages().list(userId=user_id,
                                               q=label_q_email_from,
                                               maxResults=500).execute()
    # Делаем запрос-проверку на непрочитанные
    messages = []
    if 'messages' in response:
      messages.extend(response['messages'])

    while 'nextPageToken' in response:
      page_token = response['nextPageToken']

      response = service.users().messages().list(userId=user_id,
                                                 q=label_q_email_from,
                                                 pageToken=page_token,
                                                 maxResults=500).execute()

      messages.extend(response['messages'])

      print('= > Следующий шаг обработать %d писем [page token: %s], %d обработано '
            'на текущий шаг' % (len(response['messages']), page_token, len(messages)))
      sys.stdout.flush()

    return messages

  except errors.HttpError as error:
    print('= > Произошла ошибка: %s' % error)


if __name__ == "__main__":
  print('\n = > Начинаем < = ')

  # Создаем storage.JSON с данными аутентификации
  # Применяем modify вместо readonly, т.к. у Непрочитанных будем менять статус на Прочитанное
  SCOPES = 'https://www.googleapis.com/auth/gmail.modify'
  store = file.Storage('storage.json')
  creds = store.get()

  if not creds or creds.invalid:
      flow = client.flow_from_clientsecrets('credentials.json', SCOPES)
      creds = tools.run_flow(flow, store)

  GMAIL = discovery.build('gmail', 'v1', http=creds.authorize(Http()))

  user_id =  'me' # Почта по которой делаем запросы, но т.к. она «наша», можно поставить 'me'
  label_id_one = 'INBOX' # Где ищем
  label_id_two = 'UNREAD' # Дополнительно критерий по непрочитанным, если нужен будет

  print('\n = > Формируем список всех писем < =')

  # email_list = ListMessagesWithLabels(GMAIL, user_id, [label_id_one,label_id_two])
  # Используй строку выше для анализа по непрочитанным (вынь из комментирования)
  email_list = ListMessagesWithLabels(GMAIL, user_id, [])
  # Используй строку выше для анализа по всем письмам

  final_list = [ ]

  print('\n = > фетчим все данные писем, это займёт некоторое время. Надо немного подождать.')
  sys.stdout.flush()


  # Экспортируем значения в .csv файл
  rows = 0
  file = 'Выгрузка_Писем_%s.csv' % (strftime("%Y_%m_%d_%H%M%S", gmtime()))
  with open(file, 'w', encoding='utf-8', newline = '') as csvfile:
      fieldnames = ['Subject','DateTime','Message_body']
      writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter = ',')
      writer.writeheader()

      for email in email_list:
        msg_id = email['id'] # Получаем id каждого отдельного сообщения
        email_dict = ReadEmailDetails(GMAIL,user_id,msg_id)

        if email_dict is not None:
          writer.writerow(email_dict)
          rows += 1

        if rows > 0 and (rows%50) == 0:
          print('= > итого %d писем считано' % (rows))
          sys.stdout.flush()

  print('= > Письма экспортированы в файл %s < =' % (file))
  print('\n= > Извлечено %d сообщений < =' % (rows))
  sys.stdout.flush()

  print('= > Всё готово! < =')