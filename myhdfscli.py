#Сначала: pip install requests, у меня не ставится эта собака сутулая

import requests
import os
import sys
from hdfs import InsecureClient
import json

#- requests: Библиотека для выполнения HTTP-запросов, что позволяет общаться с API WebHDFS.
#- os: Библиотека для работы с операционной системой, позволяет взаимодействовать с файловой системой.
#- sys: Библиотека для работы с параметрами командной строки.
#- json: Библиотека для работы с данными в формате JSON.- опять не ставится

class MyHDFSClient:
    def __init__(self, server, port, user):
        self.base_url = f'http://{server}:{port}/webhdfs/v1'
        self.user = user
        self.current_directory = ''

#- MyHDFSClient: Класс клиента для взаимодействия с HDFS.
#- __init__: Конструктор класса, который принимает параметры:
 # - `server`: адрес сервера HDFS.
 # - `port`: порт сервера HDFS.
 # - `user`: имя пользователя для аутентификации.
#- `self.base_url`: Формирует базовый URL для API WebHDFS.
#- `self.current_directory`: Переменная для хранения текущего каталога, изначально пустая.

    def mkdir(self, dir_name):
        url = f"{self.base_url}/{self.current_directory}{dir_name}?op=MKDIRS&user.name={self.user}"
        response = requests.put(url)
        self.handle_response(response)

# Методы соответсвующих операций
#- Создает новый каталог на HDFS.
#- Формирует URL с указанной директории и операции `MKDIRS`.
#- Отправляет PUT-запрос по сформированному URL.
#- Обрабатывает ответ с помощью метода `handle_response`.

    def put(self, local_file):
        url = f"{self.base_url}/{self.current_directory}{os.path.basename(local_file)}?op=CREATE&user.name={self.user}"
        response = requests.put(url)
        
        # Если необходимо делать редирект (307), то получим новый URL
        if response.status_code == 307:
            new_url = response.headers['Location']
            with open(local_file, 'rb') as file:
                requests.put(new_url, data=file)
            print(f"Uploaded {local_file} to HDFS.")
        else:
            self.handle_response(response)

# - Загружает файл из локальной системы в HDFS.
# - Сначала формирует URL для операции создания файла.
# - Если сервер отвечает кодом 307 (что указывает на необходимость редиректа для загрузки файла), клиент получает новый URL и загружает файл.
# - Если произошла ошибка при загрузке, обрабатывает ответ.

    def get(self, hdfs_file):
        url = f"{self.base_url}/{self.current_directory}{hdfs_file}?op=OPEN&user.name={self.user}"
        response = requests.get(url, allow_redirects=False)
        
        if response.status_code == 307:
            new_url = response.headers['Location']
            response = requests.get(new_url, allow_redirects=False)
            print(response.status_code)
            with open(hdfs_file, 'wb') as f:
                f.write(response.content)
            print(f"Downloaded {hdfs_file}.")
        else:
            self.handle_response(response)

# - Скачивает файл из HDFS на локальную систему.
# - Формирует URL для операции открытия файла.
# - Если статус код 200, то файл успешно скачан и сохраняется локально. В противном случае, ошибка обрабатывается.

    def append(self, local_file, hdfs_file):
        url = f"{self.base_url}/{self.current_directory}/{hdfs_file}?op=APPEND&user.name={self.user}"
        response = requests.post(url)

        if response.status_code == 307:
            new_url = response.headers['Location']
            with open(local_file, 'rb') as file:
                requests.put(new_url, data=file)
            print(f"Appended {local_file} to {hdfs_file}.")
        else:
            self.handle_response(response)

# - Дополняет файл в HDFS содержимым локального файла.
# - Аналогично `put`, если возникает 307 код, происходит редирект и загружается содержимое.

    def delete(self, hdfs_file):
        url = f"{self.base_url}/{self.current_directory}/{hdfs_file}?op=DELETE&user.name={self.user}"
        response = requests.delete(url)
        self.handle_response(response)

# - Удаляет файл из HDFS, используя HTTP DELETE запрос.


    def ls(self):
        url = f"{self.base_url}/{self.current_directory}?op=LISTSTATUS&user.name={self.user}"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            for file in data['FileStatuses']['FileStatus']:
                print(f"{file['type']}: {file['pathSuffix']}")
        else:
            self.handle_response(response)

    #- Выводит содержимое текущего локального каталога.

    def cd(self, dir_name):
        if dir_name == "..":
            self.current_directory = os.path.dirname(self.current_directory) or ''
        else:
            self.current_directory = os.path.join(self.current_directory, dir_name)

#- Меняет текущую локальную директорию.

    def lls(self):
        for entry in os.listdir('.'):
            print(entry)

    def lcd(self, local_dir):
        os.chdir(local_dir)
        print(f"Changed local directory to {os.getcwd()}")

    def handle_response(self, response):
        if response.status_code == 200 or response.status_code == 201:
            return
        else:
            print(f"Error: {response.status_code} - {response.text}")

# - Обрабатывает ответы от сервера.
# - Если ответ не 200 (успешно), выводит сообщение об ошибке.


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python myhdfscli.py <server> <port> <username>")
        sys.exit(1)

    server, port, username = sys.argv[1], sys.argv[2], sys.argv[3]
    client = MyHDFSClient(server, port, username)

    while True:
        command = input(f"{os.getcwd()} > ").strip().split()
        if not command:
            continue
            
        cmd = command[0]
        
        if cmd == "mkdir":
            client.mkdir(command[1])
        elif cmd == "put":
            client.put(command[1])
        elif cmd == "get":
            client.get(command[1])
        elif cmd == "append":
            client.append(command[1], command[2])
        elif cmd == "delete":
            client.delete(command[1])
        elif cmd == "ls":
            client.ls()
        elif cmd == "cd":
            client.cd(command[1])
        elif cmd == "lls":
            client.lls()
        elif cmd == "lcd":
            client.lcd(command[1])
        elif cmd == "exit":
            break
        else:
            print("Unknown command.")

# - Основная часть, которая проверяет количество аргументов командной строки и настраивает клиент.
# - В бесконечном цикле ожидает ввода команды от пользователя для выполнения операций с HDFS.


# python myhdfscli.py <server> <port> <username> пишем в командную строку
# python myhdfscli.py localhost 50070 aslebedev пример от Лебедяaking an HTTP request to a Had