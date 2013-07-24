from lovely.testlayers import server, layer
import os
import signal

here = os.path.dirname(__file__)

class CrateLayer(server.ServerLayer, layer.WorkDirectoryLayer):
    """this layer starts a crate server

    Parameters:
        name : layer name, is also used as the cluser name
        crate_home : path to home directory of the crate installation
        port : port on which crate should run
        index : a mapping containing the settings of the indices to be created
        keepRunning : do not shut down the crate instance for every
                      single test instead just delete all indices
        transport_port: port on which transport layer for crate should run
        crate_exec : alternative executable command
        crate_config : alternative crate config file location
    """

    wdClean = True

    def __init__(self,
                 name,
                 crate_home,
                 crate_config=None,
                 port=9295,
                 keepRunning=False,
                 transport_port=None,
                 crate_exec=None
                ):
        self.keepRunning = keepRunning
        crate_home = os.path.abspath(crate_home)
        servers = ['localhost:%s' % port]
        self.crate_servers = ['http://localhost:%s' % port]
        if crate_exec is None:
            crate_exec = os.path.join(crate_home, 'bin', 'crate')
        if crate_config is None:
            crate_config = os.path.join(crate_home, 'config', 'crate.yml')
        start_cmd = (
            crate_exec,
            '-f',
            '-Des.index.storage.type=memory',
            '-Des.node.name=%s' % name,
            '-Des.cluster.name=Testing%s' % port,
            '-Des.http.port=%s-%s' % (port, port),
            '-Des.network.host=localhost',
            '-Des.discovery.type=zen',
            '-Des.discovery.zen.ping.multicast.enabled=false',
            '-Des.config=%s' % crate_config,
            '-Des.path.conf=%s' % os.path.dirname(crate_config),
            )
        if transport_port:
            start_cmd += ('-Des.transport.tcp.port=%s' % transport_port,)
        super(CrateLayer, self).__init__(name, servers=servers,
                                      start_cmd=start_cmd)

    def stop(self):
        # override because if we use proc.kill the terminal gets poisioned
        self.process.send_signal(signal.SIGINT)
        self.process.wait()

    def start(self):
        self.setUpWD()
        wd = self.wdPath()
        self.start_cmd = self.start_cmd + ('-Des.path.data="%s"' % wd,)
        super(CrateLayer, self).start()


    def makeSnapshot(self, ident):
        self.es.flush()
        super(CrateLayer, self).makeSnapshot(ident)

    def restoreSnapshot(self, ident):
        self.stop()
        super(CrateLayer, self).restoreSnapshot(ident)
        self.start()

