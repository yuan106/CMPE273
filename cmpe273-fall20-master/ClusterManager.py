import consul

class ClusterManager:
    def __init__(self):
        self.c = consul.Consul()

    def listNodes(self):
        return self.c.agent.services()

    def registerNode(self, port: int):
        
        nodeName = f"server-{port}"
        print(f"Registering a server at:{port}...")

        self.c.agent.service.register(
            nodeName,
            tags=["primary", "v1"],
            port=port
        )

    def deregisterNode(self, nodeName: str):
        
        print(f"De-registering a server {nodeName}...")

        self.c.agent.service.deregister(nodeName)
