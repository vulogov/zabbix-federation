##
## Zabbix-Federation ZLM-cython module
##

from ZLM_zserver import Federation_Config_File, Federation_Server

class Federation:
    def __init__(self, ctx):
        self.ns = ctx
        self.cfg = Federation_Config_File()
    def __getitem__(self, item):
        if self.ns.item_cache.has_key(item):
            return self.ns.item_cache[item]
        iteminfo = None
        for s in self.cfg.servers():
            srv = Federation_Server(s)
            iteminfo = srv[item]
            if not iteminfo:
                continue
            return (srv, iteminfo)
        return (None,None)
    def __call__(self, ret):
        #print ret
        if ret and type(ret) == type({}) and len(ret.keys()) > 0:
            stamps = ret.keys()
            stamps.sort()
            return ret[stamps[-1]]
        else:
            return None
    def __format_json__(selfself, data):
        import simplejson as json
        return json.dumps({"data":data})
    def __merge__(self, srv, out, ret):
        import simplejson as json
        data = json.loads(ret)["data"]
        for i in ret:
            i[{"#NODE"}] = srv
            out.append(i)
        return out
    def __call_method__(self, method, param):
        out = []
        for s in self.cfg.servers():
            srv = Federation_Server(s)
            m = getattr(srv, method)
            data = apply(m, param)
            out = self.__merge__(s, out, data)
        return self.__format_json__(out)
    def history(self, item, interval, t_shift=None, fun=None):
        srv, iteminfo = self[item]
        if not srv:
            return None
        return self(srv.history(item, interval, t_shift, fun, iteminfo))
    def discovery_HostGroups(self, filter="*"):
        return self.__call_method__("discoveryHostGroup", (filter,))
    def discovery_HostInGroup(self, hg, filter="*"):
        return self.__call_method__("discoveryHostInGroup", (hg, filter))


def startup(ctx):
    ctx.item_cache = {}

def history(ctx, item, interval, t_shift=None, fun=None):
    f = Federation(ctx)
    return f.history(item, interval, t_shift, fun)

def discoveryHostGroup(ctx, filter="*"):
    f = Federation(ctx)
    return f.discovery_HostGroups(filter)



main = history

if __name__ == '__main__':
    class C:
        item_cache = {}
    c = C()
    print history(c,"zabbix-251:Context switches", "300")