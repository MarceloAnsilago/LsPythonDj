"""Default parameters for pair metrics configuration."""

DEFAULT_WINDOWS: list[int] = [80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180]
DEFAULT_BASE_WINDOW: int = 180
DEFAULT_BETA_WINDOW: int = 2
DEFAULT_ADF_MIN: float = 95.0  # percentage (p <= 0.05)
DEFAULT_ZSCORE_ABS_MIN: float = 2.0
DEFAULT_HALF_LIFE_MAX: float | None = 5.0  # dias; None desativa filtro
