# Домашнее задание 4

Продолжающееся развитие социальной сети Zwitter привело к расширению штата сотрудников, занимающихся анализом данных из социальной сети: 
основатели наняли на работу аналитиков. Основной задачей аналитиков является поиск ответов на конкретные ad-hoc бизнес-запросы;
ценность представляет, в основном конечный результат анализа, а не скорость его выполнения. Аналитики привыкли использовать в своей работе SQL.

## Что надо делать?

В четвертом домашнем задании вам необходимо помочь аналитикам включиться в работу и написать им для примера набор SQL запросов.

Ниже вы можете найти более подробную информацию по следующим вопросам:

  * [Исходные данные](#Исходные-данные)
  * [Задания для запросов](#Задания)
  * [Требования](#Требования)
  * [Процесс сдачи задания](#Процесс-сдачи-задания)
  * [Критерии сдачи задания](#Критерии-сдачи-задания)

## Исходные данные

В этом задании используются те же исходные данные, что и в первом домашнем задании: логи доступа к веб-серверу.

Из каждой записи в логе можно выделить **пользователя** и **посещенный профиль**: пользователь определяется IP-адресом,
а посещенный профиль -- идентификатором, закодированном в URI запроса в виде `idNNNNN`.

К примеру, по записи

```
195.206.123.39 - - [24/Sep/2015:12:32:53 +0400] "GET /id18222 HTTP/1.1" 200 10703 "http://bing.com/" "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.94 Safari/537.36"
```

Можно сказать, что пользователь **195.206.123.39** посетил профиль **id18222**.

Как и в первом домашнем задании, для расчета целевых метрик в запросах вам стоит рассматривать только успешные (HTTP-код 200)
запросы к веб-сайту. Неуспешные запросы стоит игнорировать.

Также в данном задании вам будут важны лайки. Иногда некоторые пользователи ставят лайк профилю. В логах это соответствует хиту с параметром `like=1`, например:

```
195.206.123.39 - - [24/Sep/2016:12:32:53 +0400] "GET /id18222?like=1 HTTP/1.1" 200 10703 "http://bing.com/" "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.94 Safari/537.36"
```

По данному хиту можно сказать, что пользователь **195.206.123.39** полайкал профиль **id18222**. Важно, что лайк **не** является посещением профиля.



## Задания

Все задания считаются для одного фиксированного дня. День будет подставляться в запрос системой проверки (см. Процесс сдачи задания).

1. `[external_table]` Напишите запрос, который создает внешнюю (EXTERNAL) таблицу с именем `access_log` c партициями по дням `day`. Добавьте одну партицию для заданного дня и добавьте запрос, который для этого дня считает величину likes per user (LPU) - среднее число лайков на юзера.

    Результат: <величина LPU>, например: 0.0067


2. `[hits_by_hours]` Из п.1 возьмите запрос, который создает внешнюю таблицу `access_log` с партицией и добавьте запрос создания обычной (managed) таблицы с именем `parsed_log` с партициями по днями (`day`) и часам (`hour`), (часовые партиции вложены в дневные) со следующими полями:

        CREATE TABLE parsed_log (
            ip STRING,
            date TIMESTAMP,
            status SMALLINT,
            url STRING,
            referer STRING
        )
        PARTITIONED BY (day STRING, hour STRING)

    В качестве формата желательно использовать RCFile или Sequencefile (`STORED AS RCFILE`, `STORED AS SEQUENCEFILE`).
    Заполните `parsed_log` из `access_log` данными за день, по `parsed_log` посчитайте число хитов за каждый час.

    Результат: <час> <число хитов>, сортировка по часам.  Например:

        00   120
        01   100
        ...


3. `[profiles_rating]` Для готовой таблицы `parsed_log` из п.2 (таблица есть и заполнена) напишите запрос, который для заданного дня выводит 3 наиболее посещаемых профиля каждого часа. Для вычисления рейтинга профилей используйте функцию RANK(), сортировка профилей по числу посещений (по убыванию), при совпадении - по алфавиту. Итоговый вывод отсортирован по часам.

    Результат: <час> <позиция в рейтинге> <профиль> <число посещений>

    Пример:

        00  1  id10011  8
        00  2  id10015  5
        00  3  id10009  2
        01  1  id10004  7
        ...


4. `[likes_by_countries]` Для готовой таблицы `parsed_log` из п.2 (таблица есть и заполнена) напишите запрос, который за данный день для каждой страны выводит число лайков, число пользователей и величину likes per user (LPU). 

    Результат отсортируйте по названию страны: <страна> <число лайков> <число пользователей> \<LPU\>

    Пример:
         Russia 10 1000 0.01
         ...


## Требования

Ваше решение должно удовлетворять следующим требованиям:

  * запросы должны быть написаны на языке HiveQL;
  * при разработке и тестировании работайте в своей базе данных, одноименной с вашим логином;
  * в сдаваемые запросы не нужно включать команды CREARE DATABASE, USE и ссылаться на таблицы из других баз;
  * написанные запросы должны честно вычислять требуемые значения, а не использовать предпосчитанные результаты.

## Процесс сдачи задания

Для самопроверки готовые запросы надо будет ввести в форму на странице: [http://hadoop2-00.yandex.ru:8888/ysda/hw4/submit](http://hadoop2-00.yandex.ru:8888/ysda/hw4/submit "submit"). Каждый запрос проверяется отдельно от других.
Вместо даты в запросе используйте конструкцию `${DATE}`. Они при проверке будут подменены на реальные даты в формате YYYY-MM-DD (NB: дата подставляется без кавычек).

В запросы не нужно включать команды CREARE DATABASE, USE и ссылаться на таблицы из других баз.

В ходе проверки ваш запрос будет исполнен, а результат -- сопоставлен с эталоном. На это уйдет несколько минут в зависимости от сложности запроса и загруженности кластера. Итог проверки будет появляться в интерфейсе по готовности на странице [http://hadoop2-00.yandex.ru:8888/ysda/hw4/result](http://hadoop2-00.yandex.ru:8888/ysda/hw4/result "result").

## Критерии сдачи задания

За корректно составленные запросы 1 и 2 выдается по два попугая за каждый. За корректные составленные запросы 3 и 4 выдается по три попугая. Суммарно в данном ДЗ можно набрать 10 попугаев.
Бонусы:

  * за решение, сданное с первого раза число попугаев удваивается (strike)
  * за решение, сданное со второго раза добавляется один попугай (spare)
  * за каждые три неудачных попытки будет вычитаться по одному попугаю (кластер не резиновый).

Срок выполнения задания - *до 20.12.2017 включительно* (+7 дней заочникам).


