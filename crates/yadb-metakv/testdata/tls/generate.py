"""Generate public TEST-ONLY TLS fixtures; never use these private keys in service.

Requires Python cryptography. CA private keys are ephemeral and not saved.
The wide fixed validity interval keeps offline tests independent of generation day.
"""
from datetime import datetime, timezone
from pathlib import Path
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID, ExtendedKeyUsageOID

OUT = Path(__file__).parent
START = datetime(2020, 1, 1, tzinfo=timezone.utc)
END = datetime(2040, 1, 1, tzinfo=timezone.utc)


def ca(name):
    key = ec.generate_private_key(ec.SECP256R1())
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
    cert = (x509.CertificateBuilder().subject_name(subject).issuer_name(subject)
            .public_key(key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(START).not_valid_after(END)
            .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
            .add_extension(x509.KeyUsage(False, False, False, False, False, True, True, False, False), critical=True)
            .sign(key, hashes.SHA256()))
    return key, cert


def leaf(name, authority, expired=False):
    key = ec.generate_private_key(ec.SECP256R1())
    ca_key, ca_cert = authority
    cert = (x509.CertificateBuilder()
            .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)]))
            .issuer_name(ca_cert.subject).public_key(key.public_key())
            .serial_number(x509.random_serial_number()).not_valid_before(START)
            .not_valid_after(datetime(2021, 1, 1, tzinfo=timezone.utc) if expired else END)
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(x509.SubjectAlternativeName([x509.DNSName(name)]), critical=False)
            .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH, ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
            .sign(ca_key, hashes.SHA256()))
    (OUT / f"{name}.cert.der").write_bytes(cert.public_bytes(serialization.Encoding.DER))
    (OUT / f"{name}.key.der").write_bytes(key.private_bytes(
        serialization.Encoding.DER, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))


trusted = ca("YADB TEST ONLY CA")
(OUT / "ca.der").write_bytes(trusted[1].public_bytes(serialization.Encoding.DER))
for node in range(1, 4):
    leaf(f"node-{node}.test", trusted)
leaf("untrusted.test", ca("YADB UNTRUSTED TEST CA"))
leaf("expired.test", trusted, expired=True)
