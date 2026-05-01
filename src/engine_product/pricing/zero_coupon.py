

def zero_coupon_yield(
    price: float,
    notional: float,
    settlement_date,
    maturity_date,
    day_count,
    ) -> float:
    
    if price <= 0:
        raise ValueError("price must be positive")

    if notional <= 0:
        raise ValueError("notional must be positive")
    
    if maturity_date <= settlement_date:
        raise ValueError("maturity_date must be after settlement_date")

    t = day_count.year_fraction(settlement_date, maturity_date)

    if t <= 0:
        raise ValueError("year fraction must be positive")

    return (notional / price) ** (1.0 / t) - 1.0
