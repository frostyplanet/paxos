import pickle
import time
from lib.net_io import NetHead

class PacketDataError (Exception):

    def __init__ (self, *args):
        Exception.__init__ (self, *args)
 

class PacketBase (object):
    cls_dict = {}
    meta_attrs = ['server_id', 'type', 'data', 'c_ts']
    attrs = []
    check_attrs = []
    meta_data = None

    def init (self, server_id):
        self.meta_data = {
            "server_id":server_id,
            "type": self.__class__.__name__,
            "data": {},
            "c_ts": time.time (),
            }
    @property
    def data (self):
        return self.meta_data["data"]

    def register (cls, *args):
        for c in args:
            cls.cls_dict[c.__name__] = c 
    register = classmethod (register)


    def deserialize (cls, sdata, expect_cls = None):
        data = None
        try:
            data = pickle.loads (sdata)
        except Exception, e:
            raise PacketDataError ("cannot deserialize")
        for i in cls.meta_attrs:
            if not data.has_key (i):
                raise PacketDataError ("key %s not found, data invalid" % (i))
        class_name = data['type']
        _clsobj = None
        if class_name:
            _clsobj = cls.cls_dict.get (class_name)
        if _clsobj is None:
            raise PacketDataError ("type %s not regconized" % (class_name))
        if expect_cls and _clsobj != expect_cls:
            raise PacketDataError ("expect %s, but got %s" % (expect_cls.__name__, class_name))
        _data = data['data']
        for i in _clsobj.check_attrs:
            if not _data.has_key (i) or _data.get(i) is None:
                raise PacketDataError ("key %s of %s not found or is None" % (i, class_name))
        obj = _clsobj ()
        obj.meta_data = data
        obj._post_deserialize ()
        return obj
    deserialize = classmethod (deserialize)

    def _post_deserialize (self):
        """ which is executed after deserialize
        """
        pass
        
    def serialize (self):
        msg_buf = pickle.dumps (self.meta_data)
        return msg_buf

    def __getitem__ (self, name):
        if name in self.meta_attrs:
            return self.meta_data[name]
        if name in self.attrs:
            return self.meta_data['data'].get (name)
        raise KeyError ("not such key %s in %s" % (name, self.__class__.__name__))

    def get (self, name):
        return self.__getitem__ (name)

    def __setitem__ (self, name, value):
        if name in self.attrs:
            self.meta_data['data'][name] = value 
            return value
        raise KeyError ("key %s is not writable in %s" % (name, self.__class__.__name__))

    def __str__ (self):
        msg = "%s[server_id:%s,%s" % (self.meta_data["type"], self.meta_data["server_id"],
                self.get_time ())
        data = self.meta_data["data"]
        for a in self.attrs:
            msg += ",%s:%s" % (a, str(data.get(a)))
        msg += "]"
        return msg

    def short_str (self):
        msg = "%s[server_id:%s,%s]" % (self.meta_data["type"], self.meta_data["server_id"],
                self.get_time ())
        return msg

    def get_time (self, ts=None):
        if ts == None:
            ts = self.meta_data["c_ts"]
        return time.strftime ("%H:%M:%S", time.localtime (ts))

# get head + serialized msg 
def serialize (data):
    head = NetHead ()
    msg_buf = data.serialize ()
    buf = head.pack (len (msg_buf)) + msg_buf
    return buf

def deserialize (buf, expect_cls=None):
    return PacketBase.deserialize (buf, expect_cls)

#######################################

class DownStreamPacket (PacketBase):
    pass

class DirectPacket (PacketBase):
    pass

#####################################################


class StatusReq (DirectPacket):

    @classmethod
    def make (cls):
        self = cls ()
        self.init ('')
        return self

    def __str__ (self):
        msg = "%s" % (self.meta_data["type"])
        return msg

class StatusResp (DirectPacket):
    attrs = ["is_master"]

    @classmethod
    def make (cls, server_id, role):
        self = cls ()
        self.init (server_id)
        data = self.meta_data["data"]
        data["role"] = role
        return self

    def __str__ (self):
        msg = "%s" % (self.meta_data["type"])
        return msg

class ControlReq (DirectPacket):
    attrs = ["cmd", "args"]
    
    @classmethod
    def make (cls, cmd, *args):
        self = cls ()
        self.init ('')
        data = self.meta_data["data"]
        data["cmd"] = cmd
        data["args"] = args
        return self


class ControlResp (DirectPacket):
    attrs = check_attrs = ["status", "message"]
    
    @classmethod
    def make (cls, status, message=""):
        self = cls ()
        self.init ('')
        data = self.meta_data["data"]
        data["status"] = status
        data["message"] = message
        return self



#################################################


for _obj in globals().values():
    if 'make' in dir (_obj) and issubclass(_obj, PacketBase):
        PacketBase.register (_obj)

        
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4 :
