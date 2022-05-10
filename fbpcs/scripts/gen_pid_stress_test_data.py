# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import random


def gen_random_id(
    token_size: int,
    lo: int,
    hi: int,
):
    # gen random value with token sized digits between lo and hi
    pii_value = random.randint(
        lo * (10 ** (token_size - 1)), hi * (10 ** (token_size - 1))
    )
    return pii_value


def gen_event_values(
    role: str,
    product: str,
):
    # add PA/PL values
    if role == "partner" and product == "lift":
        # id_,value,event_timestamp
        return "0,1600002228"
    elif role == "publisher" and product == "lift":
        # id_,test_flag,opportunity_timestamp,num_impressions,num_clicks,total_spend
        return "1,1600002544,1,3,860"
    elif role == "partner" and product == "attribution":
        # id_,conversion_timestamp,conversion_value,conversion_metadata
        return "1600000200,1001,3"
    elif role == "publisher" and product == "attribution":
        # id_,ad_id,timestamp,is_click,campaign_metadata
        return "1,1600000100,1,99"
    return ""


def gen_input(
    sample_size: int,
    token_size: int,
    lo: int,
    hi: int,
    product: str,
    partner_num_ids: int,
    partner_file: str,
    publisher_num_ids: int,
    publisher_file: str,
):
    # create files for server and client
    with open(partner_file, "a") as f1, open(publisher_file, "a") as f2:
        partner_written = 0
        publisher_written = 0
        for _i in range(0, sample_size):
            partner_pii_values = ""
            publisher_pii_values = ""

            # generate identifisers of each row
            for j in range(0, max(partner_num_ids, publisher_num_ids)):
                pii_value = gen_random_id(token_size, lo, hi)
                if j < partner_num_ids:
                    partner_pii_values += f"{pii_value},"
                if j < publisher_num_ids:
                    publisher_pii_values += f"{pii_value},"

            # only when num ids > 0, write to file
            if partner_num_ids > 0:
                partner_pii_values += f"{gen_event_values('partner', product)}"
                f1.write(f"{partner_pii_values}\n")
                partner_written += 1
            if publisher_num_ids > 0:
                publisher_pii_values += f"{gen_event_values('publisher', product)}"
                f2.write(f"{publisher_pii_values}\n")
                publisher_written += 1
        print(f"partner_written: {partner_written}\n")
        print(f"publisher_written: {publisher_written}\n")


def gen_header(
    product: str,
    role: str,
    filename: str,
    num_ids: int,
):
    with open(filename, "w") as f:
        id_header = ""
        for i in range(0, num_ids):
            id_header += f"id_{i},"
        if role == "partner" and product == "lift":
            f.write(f"{id_header}value,event_timestamp\n")
        elif role == "publisher" and product == "lift":
            f.write(
                f"{id_header}test_flag,opportunity_timestamp,num_impressions,num_clicks,total_spend\n"
            )
        elif role == "partner" and product == "attribution":
            f.write(
                f"{id_header}conversion_timestamp,conversion_value,conversion_metadata\n"
            )
        elif role == "publisher" and product == "attribution":
            f.write(f"{id_header}ad_id,timestamp,is_click,campaign_metadata\n")


def gen_all(
    base_name: str,
    product: str,
    intersect_pct: float,
    partner_sample_size: int,
    publisher_sample_size: int,
    token_size: int,
    partner_num_ids: int,
    publisher_num_ids: int,
):
    intersect = int(intersect_pct * 100)
    intersect_n = int(partner_sample_size * intersect_pct)
    if intersect_n > publisher_sample_size:
        print("ERROR: publisher_sample_size smaller than intersection size")
        return

    partner_file = (
        base_name
        + f"_{product}_client_{intersect}_{partner_sample_size}_{publisher_sample_size}_{token_size}_{partner_num_ids}_{publisher_num_ids}.csv"
    )
    publisher_file = (
        base_name
        + f"_{product}_server_{intersect}_{partner_sample_size}_{publisher_sample_size}_{token_size}_{partner_num_ids}_{publisher_num_ids}.csv"
    )

    # add header
    gen_header(product, "partner", partner_file, partner_num_ids)
    gen_header(product, "publisher", publisher_file, publisher_num_ids)

    # first add lines common between server and client
    gen_input(
        intersect_n,
        token_size,
        2,
        3,
        product,
        partner_num_ids,
        partner_file,
        publisher_num_ids,
        publisher_file,
    )

    # add lines that are not common between server and client
    # partner-side
    gen_input(
        partner_sample_size - intersect_n,
        token_size,
        1,
        2,
        product,
        partner_num_ids,
        partner_file,
        0,
        publisher_file,
    )
    # publisher-side
    gen_input(
        publisher_sample_size - intersect_n,
        token_size,
        3,
        4,
        product,
        0,
        partner_file,
        publisher_num_ids,
        publisher_file,
    )


token_size = 64
intersect_pct = 0.2

# gen stacked single-key files
stackedkey_publisher_sample_size_list = [5000000, 10000000]
stackedkey_partner_sample_size_list = [5000000]

stackedkey_partner_num_ids = 1
stackedkey_publisher_num_ids = 1
for stackedkey_publisher_sample_size in stackedkey_publisher_sample_size_list:
    for stackedkey_partner_sample_size in stackedkey_partner_sample_size_list:
        print(
            f"start: stacked key attribution files {stackedkey_publisher_sample_size} {stackedkey_partner_sample_size}"
        )
        gen_all(
            "./pid_stress_test_data/test",
            "attribution",
            intersect_pct,
            stackedkey_partner_sample_size,
            stackedkey_publisher_sample_size,
            token_size,
            stackedkey_partner_num_ids,
            stackedkey_publisher_num_ids,
        )
        print(
            f"end: stacked key attribution files {stackedkey_publisher_sample_size} {stackedkey_partner_sample_size}"
        )
        print(
            f"start: stacked key lift files {stackedkey_publisher_sample_size} {stackedkey_partner_sample_size}"
        )
        gen_all(
            "./pid_stress_test_data/test",
            "lift",
            intersect_pct,
            stackedkey_partner_sample_size,
            stackedkey_publisher_sample_size,
            token_size,
            stackedkey_partner_num_ids,
            stackedkey_publisher_num_ids,
        )
        print(
            f"end: stacked key lift files {stackedkey_publisher_sample_size} {stackedkey_partner_sample_size}"
        )

# gen multi-key files
multikey_publisher_sample_size = 5000000
multikey_partner_sample_size = 5000000
multikey_partner_num_ids_list = [*range(1, 6)]
multikey_publisher_num_ids_list = [*range(1, 13)]
for multikey_publisher_num_ids in multikey_publisher_num_ids_list:
    for multikey_partner_num_ids in multikey_partner_num_ids_list:
        print(
            f"start: multi-key attribution files {multikey_partner_num_ids} {multikey_publisher_num_ids}"
        )
        gen_all(
            "./pid_stress_test_data/test",
            "attribution",
            intersect_pct,
            multikey_partner_sample_size,
            multikey_publisher_sample_size,
            token_size,
            multikey_partner_num_ids,
            multikey_publisher_num_ids,
        )
        print(
            f"end: multi-key attribution files {multikey_partner_num_ids} {multikey_publisher_num_ids}"
        )
        print(
            f"start: multi-key lift files {multikey_partner_num_ids} {multikey_publisher_num_ids}"
        )
        gen_all(
            "./pid_stress_test_data/test",
            "lift",
            intersect_pct,
            multikey_partner_sample_size,
            multikey_publisher_sample_size,
            token_size,
            multikey_partner_num_ids,
            multikey_publisher_num_ids,
        )
        print(
            f"end: multi-key lift files {multikey_partner_num_ids} {multikey_publisher_num_ids}"
        )
