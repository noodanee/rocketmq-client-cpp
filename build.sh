#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -ex

basepath=$(
  cd $(dirname "$0")
  pwd
)

down_dir="${basepath}/tmp_down_dir"
build_dir="${basepath}/tmp_build_dir"
install_lib_dir="${basepath}/bin"
fname_spdlog="spdlog*.zip"
fname_libevent="libevent*.zip"
fname_jsoncpp="jsoncpp*.zip"
fname_zlib="zlib*.zip"
fname_gtest="googletest*.tar.gz"
fname_spdlog_down="v1.13.0.zip"
fname_libevent_down="release-2.1.12-stable.zip"
fname_jsoncpp_down="1.9.5.zip"
fname_zlib_down="1.3.1.zip"
fname_gtest_down="release-1.10.0.tar.gz"

PrintParams() {
  echo "=========================================one key build help============================================"
  echo "sh build.sh [no build spdlog:noLog] [no build libevent:noEvent] [no build json:noJson] [ execution test:test]"
  echo "usage: sh build.sh noLog noJson noEvent test"
  echo "=========================================one key build help============================================"
  echo ""
}

need_build_spdlog=1
need_build_jsoncpp=1
need_build_libevent=1
need_build_zlib=1
test=0
verbose=1
codecov=0
cpu_num=4

pasres_arguments() {
  for var in "$@"; do
    case "$var" in
    noLog)
      need_build_spdlog=0
      ;;
    noJson)
      need_build_jsoncpp=0
      ;;
    noEvent)
      need_build_libevent=0
      ;;
    noZlib)
      need_build_zlib=0
      ;;
    noVerbose)
      verbose=0
      ;;
    codecov)
      codecov=1
      ;;
    test)
      test=1
      ;;
    esac
  done

}
pasres_arguments "$@"

PrintParams() {
  echo "###########################################################################"

  if [ $need_build_spdlog -eq 0 ]; then
    echo "no need build spdlog lib"
  else
    echo "need build spdlog lib"
  fi

  if [ $need_build_libevent -eq 0 ]; then
    echo "no need build libevent lib"
  else
    echo "need build libevent lib"
  fi

  if [ $need_build_jsoncpp -eq 0 ]; then
    echo "no need build jsoncpp lib"
  else
    echo "need build jsoncpp lib"
  fi

  if [ $test -eq 1 ]; then
    echo "build unit tests"
  else
    echo "without build unit tests"
  fi

  if [ $codecov -eq 1 ]; then
    echo "run unit tests with code coverage"
  fi

  if [ $verbose -eq 0 ]; then
    echo "no need print detail logs"
  else
    echo "need print detail logs"
  fi

  echo "###########################################################################"
  echo ""
}

Prepare() {
  cd "${basepath}"

  if [ -e "${down_dir}" ]; then
    echo "${down_dir} is exist"
  else
    mkdir -p "${down_dir}"
  fi

  if [ -e ${fname_spdlog} ]; then
    mv -f ${basepath}/${fname_spdlog} "${down_dir}"
  fi

  if [ -e ${fname_libevent} ]; then
    mv -f ${basepath}/${fname_libevent} "${down_dir}"
  fi

  if [ -e ${fname_jsoncpp} ]; then
    mv -f ${basepath}/${fname_jsoncpp} "${down_dir}"
  fi

  if [ -e ${fname_gtest} ]; then
    mv -f ${basepath}/${fname_gtest} "${down_dir}"
  fi

  if [ -e "${build_dir}" ]; then
    echo "${build_dir} is exist"
  else
    mkdir -p "${build_dir}"
  fi

  if [ -e "${install_lib_dir}" ]; then
    echo "${install_lib_dir} is exist"
  else
    mkdir -p "${install_lib_dir}"
  fi
}

BuildSpdlog() {
  if [ $need_build_spdlog -eq 0 ]; then
    echo "no need build spdlog lib"
    return 0
  fi

  if [ -d "${basepath}/bin/include/spdlog" ]; then
    echo "spdlog already exist no need build test"
    return 0
  fi

  if [ -e ${down_dir}/${fname_spdlog} ]; then
    echo "${fname_spdlog} is exist"
  else
    wget "https://github.com/gabime/spdlog/archive/${fname_spdlog_down}" -O "${down_dir}/spdlog-${fname_spdlog_down}"
  fi
  unzip -o ${down_dir}/${fname_spdlog} -d "${down_dir}"

  spdlog_dir=$(ls -d ${down_dir}/spdlog* | grep -v zip)

  cp -r "${spdlog_dir}/include" "${install_lib_dir}"

  echo "build spdlog success."
}

BuildLibevent() {
  if [ $need_build_libevent -eq 0 ]; then
    echo "no need build libevent lib"
    return 0
  fi

  if [ -d "${basepath}/bin/include/event2" ]; then
    echo "spdlog already exist no need build test"
    return 0
  fi

  if [ -e ${down_dir}/${fname_libevent} ]; then
    echo "${fname_libevent} is exist"
  else
    wget "https://github.com/libevent/libevent/archive/${fname_libevent_down}" -O "${down_dir}/libevent-${fname_libevent_down}"
  fi
  unzip -o ${down_dir}/${fname_libevent} -d "${down_dir}"

  libevent_dir=$(ls -d ${down_dir}/libevent* | grep -v zip)

  echo "build libevent static #####################"
  pushd "${libevent_dir}"
  ./autogen.sh
  ./configure --disable-openssl --enable-static=yes --enable-shared=no CFLAGS=-fPIC CPPFLAGS=-fPIC --prefix="${install_lib_dir}"
  make -j $cpu_num
  make install
  popd

  echo "build libevent success."
}

BuildJsonCPP() {
  if [ $need_build_jsoncpp -eq 0 ]; then
    echo "no need build jsoncpp lib"
    return 0
  fi

  if [ -d "${basepath}/bin/include/jsoncpp" ]; then
    echo "jsoncpp already exist no need build test"
    return 0
  fi

  if [ -e ${down_dir}/${fname_jsoncpp} ]; then
    echo "${fname_jsoncpp} is exist"
  else
    wget "https://github.com/open-source-parsers/jsoncpp/archive/${fname_jsoncpp_down}" -O "${down_dir}/jsoncpp-${fname_jsoncpp_down}"
  fi
  unzip -o ${down_dir}/${fname_jsoncpp} -d "${down_dir}"

  jsoncpp_dir=$(ls -d ${down_dir}/jsoncpp* | grep -v zip)

  echo "build jsoncpp static ######################"
  cmake -S "${jsoncpp_dir}" -B "${jsoncpp_dir}/build" -DJSONCPP_WITH_POST_BUILD_UNITTEST=0 -DCMAKE_CXX_FLAGS=-fPIC -DBUILD_STATIC_LIBS=ON -DBUILD_SHARED_LIBS=OFF -DCMAKE_INSTALL_PREFIX="${install_lib_dir}"
  cmake --build "${jsoncpp_dir}/build"
  cmake --install "${jsoncpp_dir}/build"
  if [ ! -f "${install_lib_dir}/lib/libjsoncpp.a" ]; then
    echo " ./bin/lib directory is not libjsoncpp.a"
    cp "${install_lib_dir}/lib/x86_64-linux-gnu/libjsoncpp.a" "${install_lib_dir}/lib/"
  fi

  echo "build jsoncpp success."
}

BuildZlib() {
  if [ $need_build_zlib -eq 0 ]; then
    echo "no need build zlib lib"
    return 0
  fi

  if [ -d "${basepath}/bin/include/zlib" ]; then
    echo "zlib already exist no need build test"
    return 0
  fi

  if [ -e ${down_dir}/${fname_zlib} ]; then
    echo "${fname_zlib} is exist"
  else
    wget "https://github.com/madler/zlib/releases/download/v1.3.1/zlib131.zip" -O "${down_dir}/zlib-1.3.1.zip"
  fi
  unzip -o ${down_dir}/${fname_zlib} -d "${down_dir}"

  zlib_dir=$(ls -d ${down_dir}/zlib* | grep -v zip)

  echo "build zlib static ######################"
  pushd "${zlib_dir}"
  export CFLAGS="-fPIC"
  ./configure --static --prefix="${install_lib_dir}"
  make -j $cpu_num
  make install
  popd

  echo "build zlib success."
}

BuildRocketMQClient() {
  echo "============start to build rocketmq client cpp.========="
  if [ $test -eq 0 ]; then
    cmake -S "${basepath}" -B "${build_dir}" -DLibevent_USE_STATIC_LIBS=ON -DJSONCPP_USE_STATIC_LIBS=ON -DZLIB_USE_STATIC_LIBS=ON -DBUILD_STATIC_LIBS=ON -DBUILD_SHARED_LIBS=OFF
  else
    if [ $codecov -eq 1 ]; then
      cmake -S "${basepath}" -B "${build_dir}" -DLibevent_USE_STATIC_LIBS=ON -DJSONCPP_USE_STATIC_LIBS=ON -DZLIB_USE_STATIC_LIBS=ON -DBUILD_STATIC_LIBS=ON -DBUILD_SHARED_LIBS=OFF -DRUN_UNIT_TEST=ON -DCODE_COVERAGE=ON
    else
      cmake -S "${basepath}" -B "${build_dir}" -DLibevent_USE_STATIC_LIBS=ON -DJSONCPP_USE_STATIC_LIBS=ON -DZLIB_USE_STATIC_LIBS=ON -DBUILD_STATIC_LIBS=ON -DBUILD_SHARED_LIBS=OFF -DRUN_UNIT_TEST=ON
    fi
  fi
  cmake --build "${build_dir}"
  #sudo cmake --install "${build_dir}"
}

BuildGoogleTest() {
  if [ $test -eq 0 ]; then
    echo "no need build google test lib"
    return 0
  fi

  if [ -f "${install_lib_dir}/lib/libgtest.a" ]; then
    echo "libgtest already exist no need build test"
    return 0
  fi

  if [ -e ${down_dir}/${fname_gtest} ]; then
    echo "${fname_gtest} is exist"
  else
    wget "https://github.com/abseil/googletest/archive/${fname_gtest_down}" -O "${down_dir}/googletest-${fname_gtest_down}"
  fi
  tar -zxvf ${down_dir}/${fname_gtest} -C "${down_dir}" >"${down_dir}/unzipgtest.txt" 2>&1

  gtest_dir=$(ls -d ${down_dir}/googletest* | grep -v tar)

  echo "build googletest static #####################"
  cmake -S "${gtest_dir}" -B "${gtest_dir}/build" -DCMAKE_CXX_FLAGS=-fPIC -DBUILD_STATIC_LIBS=ON -DBUILD_SHARED_LIBS=OFF -DCMAKE_INSTALL_PREFIX="${install_lib_dir}"
  cmake --build "${gtest_dir}/build"
  cmake --install "${gtest_dir}/build"

  if [ ! -f "${install_lib_dir}/lib/libgtest.a" ]; then
    echo " ./bin/lib directory is not libgtest.a"
    cp ${install_lib_dir}/lib64/lib* "${install_lib_dir}/lib"
  fi
}

ExecutionTesting() {
  if [ $test -eq 0 ]; then
    echo "Build success without executing unit tests."
    return 0
  fi
  echo "############# unit test  start  ###########"
  cd "${build_dir}"
  if [ $verbose -eq 0 ]; then
    ctest
  else
    ctest -V
  fi
  echo "############# unit test  finish  ###########"
}

PrintParams
Prepare
BuildSpdlog
BuildLibevent
BuildJsonCPP
BuildZlib
BuildGoogleTest
BuildRocketMQClient
ExecutionTesting
