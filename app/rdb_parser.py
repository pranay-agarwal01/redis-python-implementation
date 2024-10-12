import os


class RDBParser:
    def __init__(self, rdb_file_path, set_key_value_callback=None):
        self.rdb_file_path = rdb_file_path
        self.rdb_file_content = self.open_rdb_file()
        self.index = 0
        self.hash_table_size = 0
        self.hash_table_exiry_set = 0
        self.set_key_value_callback = set_key_value_callback

    def open_rdb_file(self):
        if os.path.exists(self.rdb_file_path):
            with open(self.rdb_file_path, "rb") as f:
                return f.read()
        else:
            print(f"The file {self.rdb_file_path} does not exist")
            return None

    def parse_rdb_file(self):
        if self.rdb_file_content is None:
            return
        self.parse_version()
        self.index += 9
        found_hash_set = False
        while self.index < len(self.rdb_file_content):
            byte = self.rdb_file_content[self.index]
            if byte == 0xFA or byte == 0xFE:
                self.index += 1
                pass
            elif byte == 0xFF:
                print("End of file")
                self.index += 1
                break
            elif byte == 0xFD:
                print("Expiry key in seconds, not supporting for now.")
                self.index += 1
                pass
            elif byte == 0xFB:
                found_hash_set = True
                self.get_hash_table_size()
                print(
                    f"Hash table size: {self.hash_table_size}, Expiry set: {self.hash_table_exiry_set}"
                )
                self.index += 1
            elif byte == 0xFC:
                self.index += 1
                self.parse_millisecond_expiry_keys()
            elif found_hash_set:
                self.parse_normal_key_value()
            else:
                self.index += 1

    def get_hash_table_size(self):
        self.hash_table_size = int(self.rdb_file_content[self.index + 1])
        self.hash_table_exiry_set = int(self.rdb_file_content[self.index + 2])
        self.index += 2

    def parse_millisecond_expiry_keys(self):
        expiry_time = int.from_bytes(
            self.rdb_file_content[self.index : self.index + 8], "little"
        )
        self.index += 8
        key, value = self.parse_key_value()
        if self.set_key_value_callback:
            self.set_key_value_callback(key, value, expiry_time)

    def parse_normal_key_value(self):
        key, value = self.parse_key_value()
        if self.set_key_value_callback:
            self.set_key_value_callback(key, value)

    def parse_key_value(self):
        key_type = self.rdb_file_content[self.index]
        if key_type == 0x00:  # String type key encoded
            self.index += 1
            key = self.string_decoder()
            value = self.string_decoder()
            print(f"RDB -> Key: {key}, Value: {value}")
            return key, value

    def string_decoder(self):
        str_length = int(self.rdb_file_content[self.index])
        string_data = self.rdb_file_content[
            self.index + 1 : self.index + 1 + str_length
        ]
        self.index += str_length + 1
        return string_data.decode("utf-8")

    def parse_version(self):
        version = self.rdb_file_content[0:9]
        print(f"Version: {version.decode('utf-8', errors='ignore')}")
