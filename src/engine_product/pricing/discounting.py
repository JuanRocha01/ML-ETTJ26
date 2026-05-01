
def compound_discount_factor(rate: float, t: float) -> float:
    if rate <= -1.0:
        raise ValueError("rate must be greater than -1 for compound discounting")

    return (1.0 + rate) ** (-t)