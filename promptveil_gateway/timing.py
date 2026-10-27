"""Timer helpers. Release times need sub-millisecond accuracy, so the last 20 ms before every deadline are
spent busy-waiting instead of sleeping; operating-system timer coalescing otherwise adds errors of 1-16 ms."""
import ctypes
import select
import sys
import time

SPIN_NS = 20_000_000


def enable_high_resolution_timers():
    """Request 1-ms timer resolution on Windows and opt out of power throttling; harmless elsewhere."""
    try:
        ctypes.WinDLL("winmm").timeBeginPeriod(1)
    except Exception:
        pass
    try:
        class ProcessPowerThrottlingState(ctypes.Structure):
            _fields_ = [("Version", ctypes.c_ulong), ("ControlMask", ctypes.c_ulong), ("StateMask", ctypes.c_ulong)]

        # EXECUTION_SPEED | IGNORE_TIMER_RESOLUTION, both disabled
        state = ProcessPowerThrottlingState(1, 0x1 | 0x4, 0)
        kernel32 = ctypes.WinDLL("kernel32")
        kernel32.SetProcessInformation(kernel32.GetCurrentProcess(), 4, ctypes.byref(state), ctypes.sizeof(state))
    except Exception:
        pass
    sys.setswitchinterval(1e-4)


def now_ns():
    return time.perf_counter_ns()


def sleep_until(t_ns):
    while True:
        remaining = t_ns - time.perf_counter_ns()
        if remaining <= 0:
            return
        if remaining > SPIN_NS + 500_000:
            time.sleep((remaining - SPIN_NS) / 1e9)


def wait_readable_until(sock, t_ns):
    """Return True as soon as sock is readable, or False once t_ns is reached."""
    while True:
        remaining = t_ns - time.perf_counter_ns()
        if remaining <= 0:
            readable, _, _ = select.select([sock], [], [], 0)
            return bool(readable)
        if remaining > SPIN_NS + 500_000:
            readable, _, _ = select.select([sock], [], [], (remaining - SPIN_NS) / 1e9)
        else:
            readable, _, _ = select.select([sock], [], [], 0)
        if readable:
            return True
