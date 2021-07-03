/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "utility/JsonSerializer.h"

#include <memory>
#include <sstream>

#include "MQException.h"

namespace rocketmq {
namespace JsonSerializer {

namespace {

class PlainStreamWriterBuilder : public Json::StreamWriterBuilder {
 public:
  PlainStreamWriterBuilder() : StreamWriterBuilder() { (*this)["indentation"] = ""; }
};

class PowerCharReaderBuilder : public Json::CharReaderBuilder {
 public:
  PowerCharReaderBuilder() : CharReaderBuilder() { (*this)["allowNumericKeys"] = true; }
};

Json::StreamWriterBuilder& getPrettyWriterBuilder() {
  static Json::StreamWriterBuilder sPrettyWriterBuilder;
  return sPrettyWriterBuilder;
}

Json::StreamWriterBuilder& getPlainWriterBuilder() {
  static PlainStreamWriterBuilder sPlainWriterBuilder;
  return sPlainWriterBuilder;
}

Json::CharReaderBuilder& getPowerReaderBuilder() {
  static PowerCharReaderBuilder sPowerReaderBuilder;
  return sPowerReaderBuilder;
}

}  // namespace

std::string ToJson(const Json::Value& object) {
  return ToJson(object, false);
}

std::string ToJson(const Json::Value& object, bool pretty_format) {
  std::ostringstream sout;
  ToJson(object, sout, pretty_format);
  return sout.str();
}

void ToJson(const Json::Value& object, std::ostream& sout, bool pretty_format) {
  std::unique_ptr<Json::StreamWriter> writer;
  if (pretty_format) {
    writer.reset(getPrettyWriterBuilder().newStreamWriter());
  } else {
    writer.reset(getPlainWriterBuilder().newStreamWriter());
  }
  writer->write(object, &sout);
}

Json::Value FromJson(std::istream& sin) {
  std::ostringstream ssin;
  ssin << sin.rdbuf();
  std::string json = ssin.str();
  return FromJson(json);
}

Json::Value FromJson(const std::string& json) {
  const char* begin = json.data();
  const char* end = begin + json.size();
  return FromJson(begin, end);
}

Json::Value FromJson(const ByteArray& bytes) {
  const char* begin = bytes.array();
  const char* end = begin + bytes.size();
  return FromJson(begin, end);
}

Json::Value FromJson(const char* begin, const char* end) {
  Json::Value root;
  std::string errs;
  std::unique_ptr<Json::CharReader> reader(getPowerReaderBuilder().newCharReader());
  // TODO: object as key
  if (reader->parse(begin, end, &root, &errs)) {
    return root;
  }
  THROW_MQEXCEPTION(MQException, errs, -1);
}

}  // namespace JsonSerializer
}  // namespace rocketmq
