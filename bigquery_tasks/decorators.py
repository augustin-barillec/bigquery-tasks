def add_start_end_logs(f):
    def decored(self):
        self.emit_start_log_message()
        res = f(self)
        self.emit_end_log_message()
        return res
    return decored


def set_time_to_live(f):
    def decored(self):
        res = f(self)
        self.set_time_to_live()
        return res
    return decored
