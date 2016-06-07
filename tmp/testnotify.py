#!/usr/bin/env python  
# encoding:utf-8  
  
import os
import StringIO
import struct

from pyinotify import  WatchManager, Notifier, \
    ProcessEvent,IN_DELETE, IN_CREATE,IN_MODIFY 

class EventType :
    UNKNOWN_EVENT= 0
    START_EVENT_V3= 1
    QUERY_EVENT= 2
    STOP_EVENT= 3
    ROTATE_EVENT= 4
    INTVAR_EVENT= 5
    LOAD_EVENT= 6
    SLAVE_EVENT= 7
    CREATE_FILE_EVENT= 8
    APPEND_BLOCK_EVENT= 9
    EXEC_LOAD_EVENT= 10
    DELETE_FILE_EVENT= 11
    NEW_LOAD_EVENT= 12
    RAND_EVENT= 13
    USER_VAR_EVENT= 14
    FORMAT_DESCRIPTION_EVENT= 15
    XID_EVENT= 16
    BEGIN_LOAD_QUERY_EVENT= 17
    EXECUTE_LOAD_QUERY_EVENT= 18
    TABLE_MAP_EVENT = 19
    PRE_GA_WRITE_ROWS_EVENT = 20
    PRE_GA_UPDATE_ROWS_EVENT = 21
    PRE_GA_DELETE_ROWS_EVENT = 22
    WRITE_ROWS_EVENT = 23
    UPDATE_ROWS_EVENT = 24
    DELETE_ROWS_EVENT = 25
    INCIDENT_EVENT= 26
    HEARTBEAT_LOG_EVENT= 27

event_types = {}
for attr in dir(EventType):
    if attr[0].isupper():
        event_types[getattr(EventType, attr)] = attr


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


class BinFileReader(object) : 

    def __init__(self , data) :
        self.stream = StringIO.StringIO(data)

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


class EventHeader :
    EVENT_HEADER_LEN = 19
    def __init__(self , reader) :
        self.timestamp = reader.uint32()
        self.type_code = reader.uint8()
        self.server_id = reader.uint32()
        self.event_length = reader.uint32()
        self.next_position = reader.uint32()
        self.flags = reader.uint16()

        self.content = reader.chars(self.event_length)

    def __repr__(self) :
        msg = 'EventHeader: Timestamp=%s type_code=%s type=%s server_id=%s event_length=%s next_position=%s flags=%s content=\n%s' % \
              (self.timestamp  , self.type_code , event_types[self.type_code], self.server_id , self.event_length , self.next_position ,
               self.flags, self.content)
        return msg
  
class EventHandler(ProcessEvent):  
    def __init__(self, fp):
        self.fp = fp
    """事件处理"""  
    def process_IN_CREATE(self, event):  
        print   "Create file: %s "  %   os.path.join(event.path,event.name)  
  
    def process_IN_DELETE(self, event):  
        print   "Delete file: %s "  %   os.path.join(event.path,event.name)  
  
    def process_IN_MODIFY(self, event):  
        print   "Modify file: %s %s"  %   (event.path,event.name)  
        data = self.fp.read()
        print "get %d bytes" % len(data)
        print EventHeader(BinFileReader(data))
  
def FSMonitor(path='.'):  
    wm = WatchManager()   
    # mask = IN_DELETE | IN_CREATE |IN_MODIFY  
    mask = IN_MODIFY
    fp = open(path, 'r')
    fp.seek(0, os.SEEK_END)
    notifier = Notifier(wm, EventHandler(fp))  
    wm.add_watch(path, mask, rec=True)  
    print 'now starting monitor %s'%(path)  
    while True:  
        try:  
            notifier.process_events()  
            if notifier.check_events():  
                notifier.read_events()  
        except KeyboardInterrupt:  
            notifier.stop()  
            break  
    fp.close()
  
if __name__ == "__main__":  
    import sys
    FSMonitor(path = sys.argv[1])
