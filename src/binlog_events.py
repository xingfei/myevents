# coding:utf-8

import struct

'''
其中的规则为：
1. 文件开始为 FORMAT_DESCRIPTION_EVENT
2. 中间为QUERY_EVENT 和TABLE_MAP_EVENT UPDATE_ROWS_EVENT
3. TABLE_MAP_EVENT *_ROWS_EVENT 必须一起使用
4. 文件结束但Binlog不结束，ROTATE_EVENT
5. 文件结束 Binlog 也结束，END_EVENT
'''

class BinlogHeader: 
    value  = '\xfe\x62\x69\x6e'
    length = len(value)

class BinlogEvents:
    UNKNOWN_EVENT             = 0
    START_EVENT_V3            = 1
    QUERY_EVENT               = 2
    STOP_EVENT                = 3
    ROTATE_EVENT              = 4
    INTVAR_EVENT              = 5
    LOAD_EVENT                = 6
    SLAVE_EVENT               = 7
    CREATE_FILE_EVENT         = 8
    APPEND_BLOCK_EVENT        = 9
    EXEC_LOAD_EVENT           = 10
    DELETE_FILE_EVENT         = 11
    NEW_LOAD_EVENT            = 12
    RAND_EVENT                = 13
    USER_VAR_EVENT            = 14
    FORMAT_DESCRIPTION_EVENT  = 15
    XID_EVENT                 = 16
    BEGIN_LOAD_QUERY_EVENT    = 17
    EXECUTE_LOAD_QUERY_EVENT  = 18
    TABLE_MAP_EVENT           = 19
    PRE_GA_WRITE_ROWS_EVENT   = 20
    PRE_GA_UPDATE_ROWS_EVENT  = 21
    PRE_GA_DELETE_ROWS_EVENT  = 22
    WRITE_ROWS_EVENT          = 23
    UPDATE_ROWS_EVENT         = 24
    DELETE_ROWS_EVENT         = 25
    INCIDENT_EVENT            = 26
    HEARTBEAT_LOG_EVENT       = 27

binlog_events = {}
for attr in dir(BinlogEvents):
    if attr[0].isupper():
        binlog_events[getattr(BinlogEvents, attr)] = attr


class BinlogEvent:
    # header part
    timestamp     = 0 
    event_code    = 0
    event_name    = ''
    server_id     = ''
    event_length  = 0 
    next_position = 0 
    flags         = 0

    # data part
    data          = ''

    def __init__(self, header_length = 19):
        self.header_length = 19

    def fill_header(self, reader):
        self.timestamp     = reader.uint32()
        self.event_code    = reader.uint8()
        self.server_id     = reader.uint32()
        self.event_length  = reader.uint32()
        self.next_position = reader.uint32()
        self.flags         = reader.uint16()

        print '--fill header| event code :', self.event_code
        if self.event_code >= 0 and self.event_code < len(binlog_events):
            self.event_name    = binlog_events[self.event_code]
        else:
            self.event_name    = 'blablabla'

    def fill_data(self, reader):
        data_len = self.event_length - self.header_length
        if data_len > 0:
            self.data = reader.chars(data_len)

    def __repr__(self) : 
        msg = 'EventHeader: Timestamp=%s type_code=%s event_name=%s,\nserver_id=%s event_length=%s next_position=%s flags=%s' % \
              (self.timestamp  , self.event_code, self.event_name, self.server_id , self.event_length , self.next_position , 
               self.flags)
        return msg


def to_uint8(byts) : 
    retval,= struct.unpack('B' , byts)
    return retval

def to_uint16(byts) : 
    retval,= struct.unpack('H' , byts)
    return retval

def to_uint32(byts) : 
    retval,= struct.unpack('I' , byts)
    return retval

def to_uint64(byts) : 
    retval,= struct.unpack('Q' , byts)
    return retval

def to_char(byts) : 
    retval,= struct.unpack('c' , byts)
    return retval

class BinlogReader(object):
    def __init__(self , filepath) :
        self.stream = open(filepath, 'rb')

        magic = self.chars(BinlogHeader.length)
        if magic != BinlogHeader.value:
            raise Exception('invalid binlog file(magic)')

    def uint8(self)  : 
        buf = self.stream.read(1) 
        if buf is None or buf=='' : 
            raise Exception('End of file.')
        return to_uint8(buf)

    def uint16(self) : 
        buf = self.stream.read(2) 
        if buf is None or buf=='' : 
            raise Exception('End of file.')
        return to_uint16(buf) 

    def uint32(self) : 
        buf = self.stream.read(4) 
        if buf is None or buf=='' : 
            raise Exception('End of file.')
        return to_uint32(buf)

    def uint64(self) :
        buf = self.stream.read(8) 
        if buf is None or buf=='' : 
            raise Exception('End of file.')
        return to_uint64(buf)

    def char(self) : 
        buf = self.stream.read(1) 
        if buf is None or buf=='' : 
            raise Exception('End of file.')
        return to_char(buf)

    def chars(self , byte_size=1) : 
        buf = self.stream.read(byte_size) 
        if buf is None or buf=='' : 
            raise Exception('End of file.')
        return buf

    def curr_pos(self):
        return self.stream.tell()

    def close(self) : 
        self.stream.close()

    def seek(self , p , seek_type=0) : 
        self.stream.seek(p , seek_type)

class EventReader:
    def __init__(self, filereader):
        self.filereader = filereader
        self.binlog_version = 0 
        self.server_version = ''
        self.create_timestamp = 0 
        self.header_length = 0
        self.fixed_data_length = []

        self._read_format_desc()

    def _read_format_desc(self):
        e = self.read_event(True)

        byts = e.data
        i = 0
        self.binlog_version = to_uint16(byts[i: i + 2])
        print 'binlog version', self.binlog_version
        i += 2
        self.server_version = byts[i: i + 50]
        print 'server version', self.server_version
        i += 50
        self.create_timestamp = to_uint32(byts[i: i + 4])
        print 'create timestamp', self.create_timestamp
        i += 4 
        self.header_length = to_uint8(byts[i: i + 1])
        print 'header length', self.header_length
        i += 1
        for c in byts[i:i + 27]:
            l = to_uint8(c)
            self.fixed_data_length.append(l)
        print self.fixed_data_length


    def read_event(self, reading_format_desc = False):
        print '>> AT %d' % self.filereader.curr_pos()
        e = BinlogEvent()
        e.fill_header(self.filereader)
        print ' before filling data %d' % self.filereader.curr_pos()
        e.fill_data(self.filereader)

        print 'read event got', e
        return e

if __name__ == '__main__':
    import sys
    filepath = sys.argv[1]
    r = BinlogReader(filepath)
    event_reader = EventReader(r)

    while 1:
        event = event_reader.read_event()
        if event.event_code in (BinlogEvents.STOP_EVENT, BinlogEvents.ROTATE_EVENT):
            break
