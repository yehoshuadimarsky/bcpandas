import hypothesis.strategies as st
from bcpandas.constants import _DELIMITER_OPTIONS, _QUOTECHAR_OPTIONS
from hypothesis import assume
from hypothesis.extra import pandas as hpd

# Hypo - typical use cases
#   - DataFrame: at least one row
#   - Text: All text in ASCII 32-127, except the space character (32)
#   - Integers: between -2**31-1 and 2**31-1
#   - Floats: between -2**31-1 and 2**31-1, without NaN or inf

MAX_VAL = 2 ** 31 - 1

text_basic_strat = st.text(alphabet=st.characters(min_codepoint=33, max_codepoint=127), min_size=1)


hypo_df = hpd.data_frames(
    columns=[
        hpd.column(name="col1", elements=text_basic_strat),
        hpd.column(name="col2", elements=st.integers(min_value=-MAX_VAL, max_value=MAX_VAL)),
        hpd.column(
            name="col3",
            elements=st.floats(
                min_value=-MAX_VAL, max_value=MAX_VAL, allow_nan=False, allow_infinity=False
            ),
        ),
    ],
    index=hpd.range_indexes(min_size=1),
)


def not_has_all_delims(df):
    return not all(
        df.applymap(lambda x: delim in x if isinstance(x, str) else False).any().any()
        for delim in _DELIMITER_OPTIONS
    )


def not_has_all_quotechars(df):
    return not all(
        df.applymap(lambda x: qc in x if isinstance(x, str) else False).any().any()
        for qc in _QUOTECHAR_OPTIONS
    )


def assume_not_all_delims_and_quotechars(df):
    return assume(not_has_all_delims(df) and not_has_all_quotechars(df))
