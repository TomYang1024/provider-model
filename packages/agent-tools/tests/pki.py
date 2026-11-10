"""测试用的私有 CA 与服务端证书生成（P3：TLS 验证）。本模块不会被 pytest 收集。"""

from __future__ import annotations

import datetime
import ipaddress
from dataclasses import dataclass
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID


@dataclass
class Pki:
    """测试用的私有 CA 与两张服务端证书（一张给 localhost，一张给 wrong.test）。"""

    ca: Path
    good_cert: Path
    good_key: Path
    wrong_cert: Path
    wrong_key: Path


def _issue(
    ca_key: ec.EllipticCurvePrivateKey, ca_name: x509.Name, host: str, out: Path
) -> tuple[Path, Path]:
    """用 CA 签发一张服务端证书。

    Args:
        ca_key: CA 私钥。
        ca_name: CA 名称。
        host: 证书的 DNS 名称。
        out: 输出目录。

    Returns:
        ``(证书文件, 私钥文件)``。
    """
    key = ec.generate_private_key(ec.SECP256R1())
    now = datetime.datetime.now(datetime.UTC)
    san: list[x509.GeneralName] = [x509.DNSName(host)]
    if host == "localhost":
        san.append(x509.IPAddress(ipaddress.ip_address("127.0.0.1")))
    cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, host)]))
        .issuer_name(ca_name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName(san), critical=False)
        .sign(ca_key, hashes.SHA256())
    )
    cert_path, key_path = out / f"{host}.pem", out / f"{host}.key"
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return cert_path, key_path


def make_pki(out: Path) -> Pki:
    """在 ``out`` 目录下生成私有 CA 和两张服务端证书。

    Args:
        out: 输出目录。

    Returns:
        证书与私钥文件路径。
    """
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "agent-tools test CA")])
    now = datetime.datetime.now(datetime.UTC)
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(ca_key, hashes.SHA256())
    )
    ca_path = out / "ca.pem"
    ca_path.write_bytes(ca_cert.public_bytes(serialization.Encoding.PEM))
    good = _issue(ca_key, ca_name, "localhost", out)
    wrong = _issue(ca_key, ca_name, "wrong.test", out)
    return Pki(ca_path, good[0], good[1], wrong[0], wrong[1])
