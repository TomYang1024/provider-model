"""本地 stdio MCP 演示服务：提供带固定前后缀的文本回显工具。"""

from mcp.server.mcpserver import MCPServer

server = MCPServer("agent-tools-echo-demo")


@server.tool()
def echo(text: str) -> str:
    """给输入文本添加固定前后缀，用于验证 MCP 工具调用。"""
    return f"echo {text} 123"


if __name__ == "__main__":
    server.run("stdio")
