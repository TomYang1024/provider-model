"""TLS 配置的内存握手测试（P3）：不需要网络。

真实网络上的 TLS 端到端测试在 ``test_network_e2e.py``（``network`` 标记）中。这里用
``ssl.MemoryBIO`` 在内存中完成握手，验证**生产代码实际构造的** SSL 上下文（取自
``create_http_client`` 返回的客户端）：

* 配置 ``ca_bundle`` 后信任私有 CA，并按主机名校验证书；
* 默认信任库不信任私有 CA；
* 证书主机名不符时握手失败。

SNI 与证书校验使用原始主机名（而不是连接的 IP）这一点由 P1 的 ``test_http.py`` 在 httpcore 层
验证；两者合起来覆盖了“连接已校验 IP + 按主机名校验证书”的链路，但仍不等同于真实 TCP 上的握手。
"""

from __future__ import annotations

import ssl
import sys
from pathlib import Path

import pytest

from agent_tools import NetworkPolicy
from agent_tools.adapters.http import HttpAdapterConfig, create_http_client

sys.path.insert(0, str(Path(__file__).resolve().parent))
from pki import Pki, make_pki

POLICY = NetworkPolicy(allowed_hosts=frozenset({"localhost"}))


@pytest.fixture(scope="module")
def pki(tmp_path_factory: pytest.TempPathFactory) -> Pki:
    """生成私有 CA 和服务端证书。"""
    return make_pki(tmp_path_factory.mktemp("pki"))


async def client_context(ca: Path | None) -> ssl.SSLContext:
    """取出生产代码为 HTTP 适配器 / MCP Streamable HTTP 构造的客户端 SSL 上下文。

    Args:
        ca: ``ca_bundle``；``None`` 表示默认信任库。

    Returns:
        SSL 上下文。
    """
    client = create_http_client(
        HttpAdapterConfig(network=POLICY, ca_bundle=str(ca) if ca else None)
    )
    try:
        context = client._transport._pool._ssl_context  # type: ignore[attr-defined]
        assert isinstance(context, ssl.SSLContext)
        return context
    finally:
        await client.aclose()


def handshake(client_ctx: ssl.SSLContext, cert: Path, key: Path, hostname: str) -> None:
    """在内存中完成一次 TLS 握手。

    Args:
        client_ctx: 客户端上下文。
        cert: 服务端证书。
        key: 服务端私钥。
        hostname: 客户端使用的 SNI / 校验主机名。

    Raises:
        ssl.SSLError: 握手失败（例如证书不受信任、主机名不符）。
    """
    server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_ctx.load_cert_chain(cert, key)
    c_in, c_out, s_in, s_out = (ssl.MemoryBIO() for _ in range(4))
    client = client_ctx.wrap_bio(c_in, c_out, server_hostname=hostname)
    server = server_ctx.wrap_bio(s_in, s_out, server_side=True)
    done = {"client": False, "server": False}
    for _ in range(50):
        for name, side in (("client", client), ("server", server)):
            if done[name]:
                continue
            try:
                side.do_handshake()
                done[name] = True
            except ssl.SSLWantReadError:
                pass
        s_in.write(c_out.read())
        c_in.write(s_out.read())
        if all(done.values()):
            return
    raise AssertionError("handshake did not complete")


async def test_private_ca_is_trusted_and_hostname_verified(pki: Pki) -> None:
    """配置 ``ca_bundle`` 后，私有 CA 签发的 localhost 证书握手成功，且要求校验证书与主机名。"""
    context = await client_context(pki.ca)
    assert context.verify_mode == ssl.CERT_REQUIRED and context.check_hostname
    handshake(context, pki.good_cert, pki.good_key, "localhost")


async def test_default_trust_store_rejects_private_ca(pki: Pki) -> None:
    """未配置 ``ca_bundle`` 时使用默认信任库，私有 CA 不受信任，握手失败。"""
    context = await client_context(None)
    with pytest.raises(ssl.SSLCertVerificationError):
        handshake(context, pki.good_cert, pki.good_key, "localhost")


async def test_hostname_mismatch_is_rejected(pki: Pki) -> None:
    """证书签给 wrong.test 时，以 localhost 连接握手失败（主机名校验没有被关闭）。"""
    context = await client_context(pki.ca)
    with pytest.raises(ssl.SSLCertVerificationError):
        handshake(context, pki.wrong_cert, pki.wrong_key, "localhost")
