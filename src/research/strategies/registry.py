from .atr_breakout import apply_atr_breakout, get_atr_breakout_candidates
from .breakout import apply_breakout, get_breakout_candidates
from .mean_reversion import apply_mean_reversion, get_mean_reversion_candidates
from .momentum_continuation import apply_momentum_continuation, get_momentum_continuation_candidates
from .pa_breakout_retest import apply_pa_breakout_retest, get_pa_breakout_retest_candidates
from .pa_false_breakout import apply_pa_false_breakout, get_pa_false_breakout_candidates
from .pa_range_rejection import apply_pa_range_rejection, get_pa_range_rejection_candidates
from .pa_trend_pullback import apply_pa_trend_pullback, get_pa_trend_pullback_candidates
from .trend_pullback import apply_trend_pullback, get_trend_pullback_candidates

STRATEGY_REGISTRY = {
    "pa_trend_pullback": {
        "apply": apply_pa_trend_pullback,
        "generate": get_pa_trend_pullback_candidates,
    },
    "pa_breakout_retest": {
        "apply": apply_pa_breakout_retest,
        "generate": get_pa_breakout_retest_candidates,
    },
    "pa_false_breakout": {
        "apply": apply_pa_false_breakout,
        "generate": get_pa_false_breakout_candidates,
    },
    "pa_range_rejection": {
        "apply": apply_pa_range_rejection,
        "generate": get_pa_range_rejection_candidates,
    },
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
    "momentum_continuation": {
        "apply": apply_momentum_continuation,
        "generate": get_momentum_continuation_candidates,
    },
}
