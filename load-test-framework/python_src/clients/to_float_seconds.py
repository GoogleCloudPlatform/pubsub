def to_float_seconds(time_or_duration):
    return time_or_duration.seconds + float(time_or_duration.nanos) / 1000000000.0
