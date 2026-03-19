from .breakout import get_breakout_candidates, apply_breakout
from .mean_reversion import get_mean_reversion_candidates, apply_mean_reversion
from .trend_pullback import get_trend_pullback_candidates, apply_trend_pullback
from .atr_breakout import get_atr_breakout_candidates, apply_atr_breakout

STRATEGY_REGISTRY = {
    "breakout": {
        "apply": apply_breakout,
        "generate": get_breakout_candidates,
    },
    "mean_reversion": {
        "apply": apply_mean_reversion,
        "generate": get_mean_reversion_candidates,
    },
    "trend_pullback": {
        "apply": apply_trend_pullback,
        "generate": get_trend_pullback_candidates,
    },
    "atr_breakout": {
        "apply": apply_atr_breakout,
        "generate": get_atr_breakout_candidates,
    },
}
