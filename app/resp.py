class ResponseParser:
    def respSimpleString(message):
        return f"+{message}\r\n"

    def respBulkString(message):
        if message is None:
            return "$-1\r\n"
        return f"${len(message)}\r\n{message}\r\n"

    def respRDBContent(message: bytes):
        return f"${len(message)}\r\n".encode() + message

    def respArray(messages: list):
        if not messages:
            return "*0\r\n"
        return (
            f"*{len(messages)}"
            + "\r\n"
            + "".join([ResponseParser.respBulkString(message) for message in messages])
        )