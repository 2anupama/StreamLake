# StreamLake Phase 7 Sample Analytical Outputs

Generated from `data/streamlake_phase3.duckdb` by `scripts/run_phase7_analytics.py`.
These are measured local Gold values; empty metric output means no rejection telemetry was recorded.

## Total employees

```text
(10053,)
```

## Active employees

```text
(9951,)
```

## Total dependents

```text
(2999,)
```

## Company-wise active employee count

```text
('C004', 'Harbor Financial', 2046)
('C002', 'Evergreen Systems', 2012)
('C005', 'Cedar Retail Group', 1979)
('C001', 'Northstar Health', 1961)
('C003', 'Summit Manufacturing', 1953)
```

## Monthly additions

```text
(datetime.date(2026, 11, 1), 10057)
```

## Monthly deletions

```text
(datetime.date(2026, 11, 1), 102)
```

## Enrollment trend

```text
(datetime.date(2006, 11, 1), 15, Decimal('13500000.00'))
(datetime.date(2006, 12, 1), 60, Decimal('43750000.00'))
(datetime.date(2007, 1, 1), 36, Decimal('26250000.00'))
(datetime.date(2007, 2, 1), 49, Decimal('40000000.00'))
(datetime.date(2007, 3, 1), 36, Decimal('30500000.00'))
(datetime.date(2007, 4, 1), 43, Decimal('37500000.00'))
(datetime.date(2007, 5, 1), 44, Decimal('41750000.00'))
(datetime.date(2007, 6, 1), 47, Decimal('39750000.00'))
(datetime.date(2007, 7, 1), 45, Decimal('36250000.00'))
(datetime.date(2007, 8, 1), 36, Decimal('27000000.00'))
(datetime.date(2007, 9, 1), 26, Decimal('22250000.00'))
(datetime.date(2007, 10, 1), 27, Decimal('19000000.00'))
(datetime.date(2007, 11, 1), 39, Decimal('32750000.00'))
(datetime.date(2007, 12, 1), 48, Decimal('31500000.00'))
(datetime.date(2008, 1, 1), 39, Decimal('30250000.00'))
(datetime.date(2008, 2, 1), 40, Decimal('30250000.00'))
(datetime.date(2008, 3, 1), 43, Decimal('30500000.00'))
(datetime.date(2008, 4, 1), 33, Decimal('27750000.00'))
(datetime.date(2008, 5, 1), 49, Decimal('37750000.00'))
(datetime.date(2008, 6, 1), 34, Decimal('25000000.00'))
... 221 additional rows
```

## Sum-insured distribution

```text
('250k_to_499k', 1965, Decimal('491250000.00'))
('500k_to_999k', 3973, Decimal('2481000000.00'))
('1m_plus', 4013, Decimal('4998000000.00'))
```

## Policy-wise enrollment

```text
('POL-100', 'C001', 548, Decimal('423000000.00'))
('POL-400', 'C004', 531, Decimal('425000000.00'))
('POL-300', 'C004', 530, Decimal('420750000.00'))
('POL-400', 'C005', 530, Decimal('430250000.00'))
('POL-300', 'C005', 508, Decimal('403000000.00'))
('POL-100', 'C002', 503, Decimal('390000000.00'))
('POL-300', 'C003', 501, Decimal('412750000.00'))
('POL-400', 'C002', 500, Decimal('406250000.00'))
('POL-300', 'C002', 499, Decimal('389250000.00'))
('POL-400', 'C003', 493, Decimal('411250000.00'))
('POL-200', 'C002', 492, Decimal('407500000.00'))
('POL-200', 'C004', 492, Decimal('397250000.00'))
('POL-200', 'C001', 491, Decimal('395000000.00'))
('POL-100', 'C004', 484, Decimal('382000000.00'))
('POL-200', 'C003', 482, Decimal('391000000.00'))
('POL-400', 'C001', 473, Decimal('374000000.00'))
('POL-100', 'C003', 472, Decimal('379500000.00'))
('POL-100', 'C005', 469, Decimal('387000000.00'))
('POL-200', 'C005', 465, Decimal('361750000.00'))
('POL-300', 'C001', 437, Decimal('352250000.00'))
... 19 additional rows
```

## Company-wise dependent distribution

```text
('C001', 'POL-100', 'Child', 58)
('C001', 'POL-100', 'Parent', 52)
('C001', 'POL-100', 'Spouse', 50)
('C001', 'POL-200', 'Child', 53)
('C001', 'POL-200', 'Parent', 54)
('C001', 'POL-200', 'Spouse', 30)
('C001', 'POL-300', 'Child', 39)
('C001', 'POL-300', 'Parent', 32)
('C001', 'POL-300', 'Spouse', 45)
('C001', 'POL-400', 'Child', 45)
('C001', 'POL-400', 'Parent', 42)
('C001', 'POL-400', 'Spouse', 53)
('C002', 'POL-100', 'Child', 49)
('C002', 'POL-100', 'Parent', 49)
('C002', 'POL-100', 'Spouse', 66)
('C002', 'POL-200', 'Child', 53)
('C002', 'POL-200', 'Parent', 52)
('C002', 'POL-200', 'Spouse', 54)
('C002', 'POL-300', 'Child', 36)
('C002', 'POL-300', 'Parent', 55)
... 40 additional rows
```

## Additions versus deletions

```text
(datetime.date(2026, 11, 1), 10057, 102, 9955)
```

## Data-quality rejection trend

```text
(no rows)
```
