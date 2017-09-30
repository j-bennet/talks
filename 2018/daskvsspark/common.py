# -*- coding: utf-8
from collections import Counter


def format_id(customer, url, ts):
    """Create a unique id for the aggregated record."""
    return "{}|{}|{:%Y-%m-%dT%H:%M:%S}".format(url, customer, ts)


def format_metrics(visitors, page_views):
    """Create a dict of metrics."""
    return {
        "page_views": page_views,
        "visitors": visitors
    }


def format_referrers(referrers):
    """Create a dict of referrer counts."""
    counter = Counter(referrers)
    return dict(counter)
