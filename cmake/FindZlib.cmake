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
#
# Find zlib
#
# Find the zlib includes and library
#
# if you nee to add a custom library search path, do it via CMAKE_PREFIX_PATH
#
# -*- cmake -*-
# - Find Zlib
# Find the Zlib includes and library
#
# This module define the following variables:
#
#  ZLIB_FOUND, If false, do not try to use zlib.
#  ZLIB_INCLUDE_DIRS, where to find zlib.h, etc.
#  ZLIB_LIBRARIES, the libraries needed to use zlib.

# Support preference of static libs by adjusting CMAKE_FIND_LIBRARY_SUFFIXES
if(ZLIB_USE_STATIC_LIBS)
  set(_zlib_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES :${CMAKE_FIND_LIBRARY_SUFFIXES})
  if(WIN32)
    list(INSERT CMAKE_FIND_LIBRARY_SUFFIXES 0 .lib .a)
  else()
    set(CMAKE_FIND_LIBRARY_SUFFIXES .a)
  endif()
else()
  set(_zlib_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES :${CMAKE_FIND_LIBRARY_SUFFIXES})
  if(WIN32)
    list(INSERT CMAKE_FIND_LIBRARY_SUFFIXES 0 .dll .so)
  elseif(APPLE)
    set(CMAKE_FIND_LIBRARY_SUFFIXES .dylib)
  else()
    set(CMAKE_FIND_LIBRARY_SUFFIXES .so)
  endif()
endif()

set(ZLIB_INCLUDE_SEARCH_PATH /usr/local/include /usr/include)
set(ZLIB_LIBRARIES_SEARCH_PATH /usr/local/lib /usr/lib)
if(ZLIB_ROOT)
  list(INSERT ZLIB_INCLUDE_SEARCH_PATH 0 ${ZLIB_ROOT}/include)
  list(INSERT ZLIB_LIBRARIES_SEARCH_PATH 0 ${ZLIB_ROOT}/lib)
endif()

find_path(
  ZLIB_ZLIB_DIR
  NAMES zlib.h
  PATHS ${ZLIB_INCLUDE_SEARCH_PATH}
  NO_DEFAULT_PATH)

find_library(
  ZLIB_ZLIB_LIBRARY
  NAMES z
  PATHS ${ZLIB_LIBRARIES_SEARCH_PATH}
  NO_DEFAULT_PATH)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Zlib
        REQUIRED_VARS ZLIB_ZLIB_DIR
        VERSION_VAR 1.3.1
        HANDLE_COMPONENTS)

if(ZLIB_FOUND)
  set(ZLIB_INCLUDE_DIRS ${ZLIB_ZLIB_DIR})
  set(ZLIB_LIBRARIES ${ZLIB_ZLIB_LIBRARY})
endif(ZLIB_FOUND)
unset(ZLIB_ZLIB_DIR)
unset(ZLIB_ZLIB_LIBRARY)

mark_as_advanced(ZLIB_INCLUDE_DIRS ZLIB_LIBRARIES)

# Restore the original find library ordering
if(ZLIB_USE_STATIC_LIBS)
  set(CMAKE_FIND_LIBRARY_SUFFIXES ${_zlib_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES})
endif()
