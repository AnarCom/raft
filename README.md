# Http raft
## Поднятие через docker compose
Для локального поднятия через docker compose необходимо выполнить команду
```bash
gradle build jar -x test && docker compose up  --build
```
После чего поднимутся 3 сервера, к которым можно обращаться.

Порты http серверов указаны в файле docker compose.

## Локальное поднятие без докера
Для локального поднятия без докера необходимо сбилдить проект через gradle, используя команду
```bash
gradle build jar -x test
```

После чего появится файл 
 > ./build/libs/rest-raft-0.0.1.jar

Этот файл необходимо запустить командой 
> java -jar *filename*

Предварительно указав переменные окружения (для каждого из приложений они свои):
> SERVER_PORT - порт на котором работает http server
> ANOTHER_SERVERS - список серверов (raft), первый указывается хост:порт самого сервера (порт должен отличаться от порта http)
> затем должен идти список остальных серверов, разделенный запятыми (формат тот же).


## описание имеющихся ручек контроллеров
GET /log - возвращает список записей в журнале алгоритма

POST /state/{key}/{value} - устанавливает значение 

GET /state/{key} - получение значение по key
