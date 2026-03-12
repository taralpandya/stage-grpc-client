# client/fingerprint.py
import hashlib
import logging
import uuid

logger = logging.getLogger(__name__)


def _get_windows_fingerprint() -> str:
    """
    Generates a fingerprint from Windows hardware identifiers.
    Uses WMI to query CPU, motherboard, and MAC address.
    """
    import wmi
    c = wmi.WMI()

    components = []

    # CPU ID
    try:
        for cpu in c.Win32_Processor():
            if cpu.ProcessorId:
                components.append(cpu.ProcessorId.strip())
    except Exception as e:
        logger.warning(f"Could not get CPU ID: {e}")

    # Motherboard serial
    try:
        for board in c.Win32_BaseBoard():
            components.append(board.SerialNumber.strip())
    except Exception as e:
        logger.warning(f"Could not get motherboard serial: {e}")

    # MAC address of first physical network adapter
    try:
        for nic in c.Win32_NetworkAdapterConfiguration(IPEnabled=True):
            if nic.MACAddress:
                components.append(nic.MACAddress.strip())
                break
    except Exception as e:
        logger.warning(f"Could not get MAC address: {e}")

    if not components:
        raise RuntimeError("Could not gather any hardware identifiers")

    raw = "|".join(components)
    return hashlib.sha256(raw.encode()).hexdigest()


def _get_fallback_fingerprint() -> str:
    """
    Fallback fingerprint for non-Windows machines (dev/testing).
    Uses MAC address only — not as reliable but works for local dev.
    """
    mac = hex(uuid.getnode()).replace("0x", "").upper()
    return hashlib.sha256(mac.encode()).hexdigest()


def get_machine_fingerprint() -> str:
    """
    Returns a stable hardware fingerprint for this machine.
    On Windows: CPU + motherboard + MAC address
    On other platforms: MAC address only (dev/testing)
    """
    try:
        import platform
        if platform.system() == "Windows":
            return _get_windows_fingerprint()
        else:
            logger.warning("Non-Windows platform — using fallback fingerprint")
            return _get_fallback_fingerprint()
    except Exception as e:
        logger.error(f"Fingerprint generation failed: {e}")
        raise