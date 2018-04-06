# -*- coding: utf-8
import pandas as pd


def set_display_options():
    pd.set_option('display.max_colwidth', 1000)
    pd.set_option('display.expand_frame_repr', False)


def format_id(customer, url, ts):
    """Create a unique id for the aggregated record."""
    return "{}|{}|{:%Y-%m-%dT%H:%M:%S}".format(url, customer, ts)


def format_metrics(page_views, visitors):
    """Create a dict of metrics."""
    return {
        "page_views": page_views,
        "visitors": visitors
    }
