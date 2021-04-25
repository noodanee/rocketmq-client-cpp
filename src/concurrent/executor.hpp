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
#ifndef ROCKETMQ_CONCURRENT_EXECUTOR_HPP_
#define ROCKETMQ_CONCURRENT_EXECUTOR_HPP_

#include <functional>
#include <future>
#include <memory>
#include <queue>
#include <utility>

#include "concurrent/thread_group.hpp"
#include "concurrent/time.hpp"
#include "concurrent/unbounded_queue.hpp"

namespace rocketmq {

class thread_pool_executor {
 public:
  using handler_type = std::function<void()>;

  explicit thread_pool_executor(std::size_t thread_nums, bool start_immediately = true) : thread_nums_(thread_nums) {
    if (start_immediately) {
      startup();
    }
  }
  explicit thread_pool_executor(const std::string& name, std::size_t thread_nums, bool start_immediately = true)
      : thread_nums_(thread_nums), thread_group_(name) {
    if (start_immediately) {
      startup();
    }
  }

  void startup() {
    if (state_ == state::STOP) {
      state_ = state::RUNNING;
      thread_group_.create_threads([this]() { run(); }, thread_nums_);
      thread_group_.start();
    }
  }

  void shutdown(bool immediately = true) {
    if (state_ == state::RUNNING) {
      state_ = immediately ? state::STOP : state::SHUTDOWN;
      wakeup_event_.notify_all();
      thread_group_.join();
      state_ = state::STOP;
    }
  }

  bool is_shutdown() { return state_ != RUNNING; }

  std::size_t thread_nums() const { return thread_nums_; }
  void set_thread_nums(std::size_t thread_nums) { thread_nums_ = thread_nums; }

  void execute(handler_type command) {
    if ((state_ & kAcceptNewTasks) == 0U) {
      throw std::logic_error("executor don't accept new tasks.");
    }

    task_queue_.push(std::move(command));
    if (free_threads_ > 0) {
      wakeup_event_.notify_one();
    }
  }

  template <typename Function, typename... Arguments>
  std::future<typename std::result_of<Function(Arguments...)>::type> submit(Function&& function,
                                                                            Arguments&&... arguments) {
    using result_t = typename std::result_of<Function(Arguments...)>::type;
    auto task = std::bind(std::forward<Function>(function), std::forward<Arguments>(arguments)...);
    auto promise = std::make_shared<std::promise<result_t>>();
    auto future = promise->get_future();
    execute(wrap_task<result_t>(std::move(promise), std::move(task)));
    return future;
  }

 protected:
  template <typename Result, typename std::enable_if<std::is_same<Result, void>::value, int>::type = 0>
  handler_type wrap_task(std::shared_ptr<std::promise<Result>> promise, std::function<Result()> task) {
    return
#if __cplusplus >= 201402L
        [promise = std::move(promise), task = std::move(task)]
#else
        [promise, task]
#endif
        () {
          try {
            // handler that may throw
            task();
            promise->set_value();
          } catch (...) {
            try {
              // store anything thrown in the promise
              promise->set_exception(std::current_exception());
            } catch (...) {
            }  // set_exception() may throw too
          }
        };
  }

  template <typename Result, typename std::enable_if<!std::is_same<Result, void>::value, int>::type = 0>
  handler_type wrap_task(std::shared_ptr<std::promise<Result>> promise, std::function<Result()> task) {
    return
#if __cplusplus >= 201402L
        [promise = std::move(promise), task = std::move(task)]
#else
        [promise, task]
#endif
        () {
          try {
            // handler that may throw
            promise->set_value(task());
          } catch (...) {
            try {
              // store anything thrown in the promise
              promise->set_exception(std::current_exception());
            } catch (...) {
            }  // set_exception() may throw too
          }
        };
  }

 private:
  static const unsigned int kAcceptNewTasks = 1U << 0U;
  static const unsigned int kProcessQueuedTasks = 1U << 1U;

  enum state { STOP = 0, SHUTDOWN = kProcessQueuedTasks, RUNNING = kAcceptNewTasks | kProcessQueuedTasks };

  void run() {
    handler_type task;
    while ((state_ & kProcessQueuedTasks) != 0U) {
      if (task_queue_.try_pop(task) == queue_op_status::success) {
        try {
          task();
        } catch (...) {
        }
      } else {
        if ((state_ & kAcceptNewTasks) == 0U) {
          // don't accept new tasks
          break;
        }

        // wait new tasks
        std::unique_lock<std::mutex> lock(wakeup_mutex_);
        if (task_queue_.is_empty()) {
          free_threads_++;
          wakeup_event_.wait_for(lock, std::chrono::seconds(5));
          free_threads_--;
        }
      }
    }
  }

 private:
  unbounded_queue<handler_type> task_queue_;

  std::mutex wakeup_mutex_;
  std::condition_variable wakeup_event_;
  std::atomic<int> free_threads_{0};

  state state_{state::STOP};

  std::size_t thread_nums_;
  thread_group thread_group_;
};

namespace detail {

struct scheduled_task {
  using handler_type = thread_pool_executor::handler_type;

  handler_type task;
  std::chrono::steady_clock::time_point wakeup_time;

  scheduled_task(handler_type handler, const std::chrono::steady_clock::time_point& time)
      : task(std::move(handler)), wakeup_time(time) {}

  bool operator<(const scheduled_task& other) const { return (wakeup_time > other.wakeup_time); }

  static bool less(const scheduled_task& a, const scheduled_task& b) { return a < b; }
};

}  // namespace detail

class scheduled_thread_pool_executor : public thread_pool_executor {
 public:
  using scheduled_task = detail::scheduled_task;

  explicit scheduled_thread_pool_executor(std::size_t thread_nums, bool start_immediately = true)
      : thread_pool_executor(thread_nums, false) {
    timer_thread_.set_target(&scheduled_thread_pool_executor::time_daemon, this);
    if (start_immediately) {
      startup();
    }
  }
  explicit scheduled_thread_pool_executor(const std::string& name,
                                          std::size_t thread_nums,
                                          bool start_immediately = true)
      : thread_pool_executor(name, thread_nums, false), timer_thread_(name + "-Timer") {
    timer_thread_.set_target(&scheduled_thread_pool_executor::time_daemon, this);
    if (start_immediately) {
      startup();
    }
  }

  explicit scheduled_thread_pool_executor(bool start_immediately = true)
      : thread_pool_executor(0, false), single_thread_(true), timer_thread_() {
    timer_thread_.set_target(&scheduled_thread_pool_executor::time_daemon, this);
    if (start_immediately) {
      startup();
    }
  }
  explicit scheduled_thread_pool_executor(const std::string& name, bool start_immediately = true)
      : thread_pool_executor(name, 0, false), single_thread_(true), timer_thread_(name + "-Timer") {
    timer_thread_.set_target(&scheduled_thread_pool_executor::time_daemon, this);
    if (start_immediately) {
      startup();
    }
  }

  void startup() {
    if (stopped_) {
      stopped_ = false;

      // startup task threads
      if (!single_thread_) {
        thread_pool_executor::startup();
      }

      // start time daemon
      timer_thread_.start();
    }
  }

  void shutdown(bool immediately = true) {
    if (!stopped_) {
      stopped_ = true;

      time_event_.notify_one();
      timer_thread_.join();

      if (!single_thread_) {
        thread_pool_executor::shutdown(immediately);
      }
    }
  }

  bool is_shutdown() const { return stopped_; }

  template <typename Function, typename... Arguments>
  std::future<typename std::result_of<Function(Arguments...)>::type> submit(Function&& function,
                                                                            Arguments&&... arguments) {
    if (!single_thread_) {
      return thread_pool_executor::submit(std::forward<Function>(function), std::forward<Arguments>(arguments)...);
    }
    return schedule(std::forward<Function>(function), 0, time_unit::milliseconds,
                    std::forward<Arguments>(arguments)...);
  }

  template <typename Function, typename... Arguments>
  std::future<typename std::result_of<Function(Arguments...)>::type> schedule(Function&& function,
                                                                              long delay,
                                                                              time_unit unit,
                                                                              Arguments&&... arguments) {
    using result_t = typename std::result_of<Function(Arguments...)>::type;
    auto task = std::bind(std::forward<Function>(function), std::forward<Arguments>(arguments)...);
    auto promise = std::make_shared<std::promise<result_t>>();
    auto future = promise->get_future();
    auto time_point = until_time_point(delay, unit);
    scheduled_task wrapped_task(wrap_task<result_t>(std::move(promise), std::move(task)), time_point);
    {
      std::unique_lock<std::mutex> lock(time_mutex_);
      if (time_queue_.empty() || time_queue_.top().wakeup_time < time_point) {
        time_queue_.push(std::move(wrapped_task));
        time_event_.notify_one();
      } else {
        time_queue_.push(std::move(wrapped_task));
      }
    }
    return future;
  }

 protected:
  void time_daemon() {
    std::unique_lock<std::mutex> lock(time_mutex_);
    while (!stopped_) {
      auto now = std::chrono::steady_clock::now();
      while (!time_queue_.empty()) {
        auto& top = const_cast<scheduled_task&>(time_queue_.top());
        if (top.wakeup_time <= now) {
          if (!single_thread_) {
            thread_pool_executor::execute(std::move(top.task));
            time_queue_.pop();
          } else {
            auto copy = std::move(top.task);
            time_queue_.pop();
            lock.unlock();
            copy();
            lock.lock();

            // if function cost more time, we need re-watch clock
            now = std::chrono::steady_clock::now();
          }
        } else {
          break;
        }
      }

      if (!time_queue_.empty()) {
        const auto& top = time_queue_.top();
        // wait more 10 milliseconds
        time_event_.wait_for(lock, top.wakeup_time - now + std::chrono::milliseconds(10));
      } else {
        // default, wakeup after 10 seconds for check stopped flag.
        time_event_.wait_for(lock, std::chrono::seconds(10));
      }
    }
  }

 protected:
  std::priority_queue<scheduled_task,
                      std::vector<scheduled_task>,
                      std::function<bool(const scheduled_task&, const scheduled_task&)>>
      time_queue_{&scheduled_task::less};

 private:
  bool stopped_{true};

  bool single_thread_{false};
  thread timer_thread_;

  std::mutex time_mutex_;
  std::condition_variable time_event_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CONCURRENT_EXECUTOR_HPP_
