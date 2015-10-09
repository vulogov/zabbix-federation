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
    def history(self, item, interval, t_shift=None, fun=None):
        srv, iteminfo = self[item]
        if not srv:
            return None
        return self(srv.history(item, interval, t_shift, fun, iteminfo))



def startup(ctx):
    ctx.item_cache = {}

def history(ctx, item, interval, t_shift=None, fun=None):
    f = Federation(ctx)
    return f.history(item, interval, t_shift, fun)


main = history

if __name__ == '__main__':
    class C:
        item_cache = {}
    c = C()
    print history(c,"zabbix-251:Context switches", "300")