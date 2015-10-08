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
    def history(self, item, interval, t_shift=None, fun=None):
        srv, iteminfo = self[item]
        if not srv:
            return None
        return srv.history(item, interval, t_shift, fun, iteminfo)



def startup(ctx):
    ns.item_cache = {}

def history(ctx, item, interval, t_shift=None, fun=None):
    f = Federation(ctx)


main = history