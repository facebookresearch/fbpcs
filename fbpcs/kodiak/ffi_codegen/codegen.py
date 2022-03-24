#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import subprocess
import sys
from dataclasses import dataclass
from typing import Optional


@dataclass
class TypeInfo:
    arg_name: str
    cpp_name: str
    rust_name: str
    cpp_clear_type: str
    mpc_engine_type: str


@dataclass
class OperatorInfo:
    name: str
    symbol: str
    ret: Optional[str] = None


# TODO: Changing all these `false` to `true` makes it a batch game
BOOLEAN_TYPE = TypeInfo(
    arg_name="mpc_bool",
    cpp_name="CppMPCBool",
    rust_name="bool",
    cpp_clear_type="bool",
    mpc_engine_type="SecBit<false>",
)

ARITHMETIC_TYPES = [
    TypeInfo(
        arg_name="mpc_int32",
        cpp_name="CppMPCInt32",
        rust_name="i32",
        cpp_clear_type="int32_t",
        mpc_engine_type="SecSignedInt<32, false>",
    ),
    TypeInfo(
        arg_name="mpc_int64",
        cpp_name="CppMPCInt64",
        rust_name="i64",
        cpp_clear_type="int64_t",
        mpc_engine_type="SecSignedInt<64, false>",
    ),
    TypeInfo(
        arg_name="mpc_uint32",
        cpp_name="CppMPCUInt32",
        rust_name="u32",
        cpp_clear_type="uint32_t",
        mpc_engine_type="SecUnsignedInt<32, false>",
    ),
    TypeInfo(
        arg_name="mpc_uint64",
        cpp_name="CppMPCUInt64",
        rust_name="u64",
        cpp_clear_type="uint64_t",
        mpc_engine_type="SecUnsignedInt<64, false>",
    ),
]

BOOLEAN_OPS = [
    OperatorInfo(name="and", symbol="&"),
    OperatorInfo(name="or", symbol="||"),
    OperatorInfo(name="xor", symbol="^"),
]

ARITHMETIC_OPS = [
    OperatorInfo(name="add", symbol="+"),
    OperatorInfo(name="sub", symbol="-"),
    # OperatorInfo(name="mul", symbol="*"),
    # OperatorInfo(name="div", symbol="/"),
]

COMPARISON_OPS = [
    # OperatorInfo(name="neq", symbol="!="),
    OperatorInfo(name="eq", symbol="==", ret=BOOLEAN_TYPE),
    OperatorInfo(name="lt", symbol="<", ret=BOOLEAN_TYPE),
    OperatorInfo(name="gt", symbol=">", ret=BOOLEAN_TYPE),
    OperatorInfo(name="lte", symbol="<=", ret=BOOLEAN_TYPE),
    OperatorInfo(name="gte", symbol=">=", ret=BOOLEAN_TYPE),
]


def get_license_and_generated_header() -> str:
    return (
        "/*\n"
        " * Copyright (c) Meta Platforms, Inc. and affiliates.\n"
        " *\n"
        " * This source code is licensed under the MIT license found in the\n"
        " * LICENSE file in the root directory of this source tree.\n"
        # Note: we split this @gen over two lines because otherwise Phabricator
        # will think *this* file is generated :)
        " */\n\n/* @gen"
        "erated file - do not modify directly! */\n\n"
    )


def get_h_pragma_and_includes() -> str:
    return (
        "#pragma once\n\n"
        "#include <map>\n"
        "#include <memory>\n"
        "#include <string>\n\n"
        "#include <fbpcf/engine/communication/SocketPartyCommunicationAgentFactory.h>\n"
        "#include <fbpcf/frontend/mpcGame.h>\n"
        "#include <fbpcf/mpc_std_lib/oram/IWriteOnlyOram.h>\n"
        "#include <fbpcf/mpc_std_lib/oram/LinearOramFactory.h>\n"
        "#include <fbpcf/scheduler/IScheduler.h>\n"
        "#include <fbpcf/scheduler/SchedulerHelper.h>\n"
    )


def get_using_declaration(type_info: TypeInfo) -> str:
    return (
        f"using {type_info.cpp_name} = typename fbpcf::frontend::"
        # TODO: Only handles schedulerId=0
        f"MpcGame<0>::template {type_info.mpc_engine_type};"
    )


def get_kodiak_game_classes() -> str:
    return (
        "constexpr int32_t PUBLISHER_ROLE = 0;\n"
        "constexpr int32_t PARTNER_ROLE = 1;\n\n"
        "template <int schedulerId, bool batched = false>\n"
        "class KodiakGameDetail : public fbpcf::frontend::MpcGame<schedulerId> {\n"
        " public:\n"
        "  explicit KodiakGameDetail(std::unique_ptr<fbpcf::scheduler::IScheduler> scheduler)\n"
        "      : fbpcf::frontend::MpcGame<schedulerId>(std::move(scheduler)) {}\n"
        "};\n"
        # TODO: Adding ', true' as a second template argument makes this a batch game
        "class KodiakGame : public KodiakGameDetail<0> {\n"
        " public:\n"
        "  explicit KodiakGame(std::unique_ptr<fbpcf::scheduler::IScheduler> scheduler)\n"
        "      : KodiakGameDetail<0>(std::move(scheduler)) {}\n"
        "};\n"
        "std::unique_ptr<KodiakGame> new_kodiak_game(int32_t role, const std::string& host, int16_t port) {\n"
        "  std::map<int, fbpcf::engine::communication::SocketPartyCommunicationAgentFactory::PartyInfo> partyInfos{"
        "  {{PUBLISHER_ROLE, {host, port}}, {PARTNER_ROLE, {host, port}}}};\n"
        "  auto commAgentFactory = std::make_unique<fbpcf::engine::communication::SocketPartyCommunicationAgentFactory>(role, std::move(partyInfos));\n"
        "  auto scheduler = fbpcf::scheduler::createLazySchedulerWithRealEngine(role, *commAgentFactory);\n"
        "  return std::make_unique<KodiakGame>(std::move(scheduler));\n"
        "}\n"
    )


def func_to_header_declaration(f: str) -> str:
    return f[: f.index("{") - 1] + ";"


def make_new_func(type_info: TypeInfo) -> str:
    cpp_typename = type_info.cpp_name
    arg_name = type_info.arg_name
    clear_type = type_info.cpp_clear_type
    return (
        # Signature and funcname
        f"std::unique_ptr<{cpp_typename}> new_{arg_name}"
        # parameters
        f"({clear_type} a, int32_t partyId) {{\n"
        # statements
        f"  return std::make_unique<{cpp_typename}>(a, partyId);\n"
        # end of func
        "}"
    )


def make_reveal_func(type_info: TypeInfo) -> str:
    ret_type = type_info.cpp_clear_type
    arg_name = type_info.arg_name
    cpp_typename = type_info.cpp_name
    return (
        # Signature and funcname
        f"{ret_type} reveal_{arg_name}"
        # parameters
        f"(const {cpp_typename}& a) {{\n"
        # statements
        # TODO: Open to *both* parties
        f"  auto res = a.openToParty(0);\n"
        f"  return res.getValue();\n"
        # end of func
        "}"
    )


def make_binop_func(type_info: TypeInfo, op_info: OperatorInfo) -> str:
    # If the operator explicitly defines a return type, use that
    # otherwise fall back to assuming A <op> A -> A
    param_typename = type_info.cpp_name
    if op_info.ret is not None:
        ret_typename = op_info.ret.cpp_name
    else:
        ret_typename = param_typename

    funcname = f"{type_info.arg_name}_{op_info.name}"
    op = op_info.symbol
    return (
        # Signature and funcname
        f"std::unique_ptr<{ret_typename}> {funcname}"
        # parameters
        f"(const {param_typename}& a, const {param_typename}& b) {{\n"
        # statements
        f"  return std::make_unique<{ret_typename}>(a {op} b);\n"
        # end of func
        "}"
    )


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit(f"Usage: python3 {sys.argv[0]} outfile")

    header_filename = sys.argv[1] + ".h"
    output_filename = sys.argv[1] + ".cpp"

    with open(header_filename, "w") as f_h, open(output_filename, "w") as f_cpp:
        # First write the license, include header, and namespace declarations
        # This is the "set up" for the h file
        print(get_license_and_generated_header(), file=f_h)
        print(get_h_pragma_and_includes(), file=f_h)
        print("namespace kodiak_cpp {\n", file=f_h)
        for type_info in [BOOLEAN_TYPE] + ARITHMETIC_TYPES:
            print(get_using_declaration(type_info), file=f_h)
        print(get_kodiak_game_classes(), file=f_h)

        # First write the license, include header, and namespace declarations
        # This is the "set up" for the cpp file
        print(get_license_and_generated_header(), file=f_cpp)
        print('#include "fbpcs/kodiak/include/ffi.h"\n', file=f_cpp)
        print("#include <memory>\n", file=f_cpp)
        print("using namespace kodiak_cpp;\n", file=f_cpp)

        # Write all the functions for the boolean type
        type_info = BOOLEAN_TYPE
        print(f"Gen functions for {type_info.arg_name}")
        new_f = make_new_func(type_info)
        print(func_to_header_declaration(new_f), file=f_h)
        print(new_f, file=f_cpp)
        reveal_f = make_reveal_func(type_info)
        print(func_to_header_declaration(reveal_f), file=f_h)
        print(reveal_f, file=f_cpp)
        for operator_info in BOOLEAN_OPS:
            binop_f = make_binop_func(type_info, operator_info)
            print(func_to_header_declaration(binop_f), file=f_h)
            print(binop_f, file=f_cpp)

        # Write all the functions for the arithmetic types
        for type_info in ARITHMETIC_TYPES:
            print(f"Gen functions for {type_info.arg_name}")
            new_f = make_new_func(type_info)
            print(func_to_header_declaration(new_f), file=f_h)
            print(new_f, file=f_cpp)
            reveal_f = make_reveal_func(type_info)
            print(func_to_header_declaration(reveal_f), file=f_h)
            print(reveal_f, file=f_cpp)
            for operator_info in ARITHMETIC_OPS:
                binop_f = make_binop_func(type_info, operator_info)
                print(func_to_header_declaration(binop_f), file=f_h)
                print(binop_f, file=f_cpp)
            for operator_info in COMPARISON_OPS:
                binop_f = make_binop_func(type_info, operator_info)
                print(func_to_header_declaration(binop_f), file=f_h)
                print(binop_f, file=f_cpp)

        print("} // namespace kodiak_cpp", file=f_h)

    # Finally, run the auto-formatters on the code
    subprocess.run(["clang-format", "-i", header_filename])
    subprocess.run(["clang-format", "-i", output_filename])


if __name__ == "__main__":
    main()
