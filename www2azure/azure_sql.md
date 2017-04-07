SELECT
    [event_id],
    [event_type],
    [user_id],
    [rate],
    [device_id],
    [device_type],
    [time_stamp],
    [training_mode],
    [quality]
INTO
    [heartrate]
FROM
    [itr2-data]
where
    [event_type] = 'heartRate'

SELECT
    [event_id],
    [event_type],
    [user_id],
    [interval],
    [device_id],
    [device_type],
    [time_stamp],
    [training_mode]
INTO
    [rrinterval]
FROM
    [itr2-data]
where
    [event_type] = 'rrinterval'

SELECT
    [event_id],
    [event_type],
    [user_id],
    [device_id],
    [device_type],
    [time_stamp],
    [training_mode],
    [temperature]
INTO
    [skintemperature]
FROM
    [itr2-data]
where
    [event_type] = 'skinTemperature'
