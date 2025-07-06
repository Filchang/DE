create table logs.data_load_log (
    id serial primary key,
    file_name varchar(255) not null, -- имя csv файла
    load_date date not null,         -- дата загрузки 
    start_time time not null,   -- время начала загрузки
    end_time time,              -- время окончания загрузки
    duration_sec float,            -- длительность загрузки (в секундах)
    count_row integer,               -- кол-во загруженных строк
    error_message text               -- сообщение об ошибке (если было)
);
