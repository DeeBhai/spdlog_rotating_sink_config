#ifndef COMPRESSED_ROTATING_SINK_H
#define COMPRESSED_ROTATING_SINK_H

#include <spdlog/details/file_helper.h>
#include <spdlog/details/null_mutex.h>
#include <spdlog/details/synchronous_factory.h>
#include <spdlog/sinks/base_sink.h>

#include <algorithm>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <regex>
#include <string>
using namespace boost::filesystem;
using namespace std;

#include "shared.h"

namespace spdlog {
namespace sinks {

//
// Rotating file sink based on size
//
template <typename Mutex>
class compressed_rotating_file_sink final : public base_sink<Mutex> {
 public:
  compressed_rotating_file_sink(filename_t base_filename, std::size_t max_size, std::size_t max_files, std::size_t max_comp_files, bool rotate_on_open = false);
  static filename_t calc_filename(const filename_t& filename, std::size_t index);
  const filename_t& filename() const;

 protected:
  void sink_it_(const details::log_msg& msg) override;
  void flush_() override;

 private:
  // Rotate files:
  // log.txt -> log.1.txt
  // log.1.txt -> log.2.txt
  // log.2.txt -> log.3.txt
  // log.3.txt -> delete
  void rotate_();
  void compress_();

  // delete the target if exists, and rename the src file  to target
  // return true on success, false otherwise.
  bool rename_file(const filename_t& src_filename, const filename_t& target_filename);

  filename_t base_filename_;
  std::size_t max_size_;
  std::size_t max_files_;
  std::size_t max_compressed_files_;
  std::size_t current_size_;
  details::file_helper file_helper_;
  path dir_;
  filename_t basename_;
  filename_t file_ext_;
};

using compressed_rotating_file_sink_mt = compressed_rotating_file_sink<std::mutex>;
using compressed_rotating_file_sink_st = compressed_rotating_file_sink<details::null_mutex>;

template <typename Mutex>
SPDLOG_INLINE compressed_rotating_file_sink<Mutex>::compressed_rotating_file_sink(filename_t base_filename, std::size_t max_size, std::size_t max_files, std::size_t max_comp_files,
                                                                                  bool rotate_on_open)
    : base_filename_(std::move(base_filename)), max_size_(max_size), max_files_(max_files), max_compressed_files_(max_comp_files) {
  file_helper_.open(calc_filename(base_filename_, 0));
  current_size_ = file_helper_.size();  // expensive. called only once
  path p(base_filename_);
  dir_ = p.parent_path();
  filename_t file_name = p.filename().string();
  std::tie(basename_, file_ext_) = details::file_helper::split_by_extension(file_name);
  if (rotate_on_open && current_size_ > 0) {
    rotate_();
    compress_();
  }
}

// calc filename according to index and file extension if exists.
// e.g. calc_filename("logs/mylog.txt, 3) => "logs/mylog.3.txt".
template <typename Mutex>
SPDLOG_INLINE filename_t compressed_rotating_file_sink<Mutex>::calc_filename(const filename_t& filename, std::size_t index) {
  if (index == 0u) {
    return filename;
  }

  filename_t basename, ext;
  std::tie(basename, ext) = details::file_helper::split_by_extension(filename);
  return fmt::format(SPDLOG_FILENAME_T("{}.{}{}"), basename, index, ext);
}

template <typename Mutex>
SPDLOG_INLINE const filename_t& compressed_rotating_file_sink<Mutex>::filename() const {
  return file_helper_.filename();
}

template <typename Mutex>
SPDLOG_INLINE void compressed_rotating_file_sink<Mutex>::sink_it_(const details::log_msg& msg) {
  memory_buf_t formatted;
  base_sink<Mutex>::formatter_->format(msg, formatted);
  current_size_ += formatted.size();
  if (current_size_ > max_size_) {
    rotate_();
    compress_();
    current_size_ = formatted.size();
  }
  file_helper_.write(formatted);
}

template <typename Mutex>
SPDLOG_INLINE void compressed_rotating_file_sink<Mutex>::flush_() {
  file_helper_.flush();
}

// Rotate files:
// log.txt -> log.1.txt
// log.1.txt -> log.2.txt
// log.2.txt -> log.3.txt
// log.3.txt -> delete
template <typename Mutex>
SPDLOG_INLINE void compressed_rotating_file_sink<Mutex>::rotate_() {
  using details::os::filename_to_str;
  using details::os::path_exists;
  file_helper_.close();
  for (auto i = max_files_; i > 0; --i) {
    filename_t src = calc_filename(base_filename_, i - 1);
    if (!path_exists(src)) {
      continue;
    }
    filename_t target = calc_filename(base_filename_, i);

    if (!rename_file(src, target)) {
      // if failed try again after a small delay.
      // this is a workaround to a windows issue, where very high rotation
      // rates can cause the rename to fail with permission denied (because of antivirus?).
      details::os::sleep_for_millis(100);
      if (!rename_file(src, target)) {
        file_helper_.reopen(true);  // truncate the log file anyway to prevent it to grow beyond its limit!
        current_size_ = 0;
        SPDLOG_THROW(spdlog_ex("rotating_file_sink: failed renaming " + filename_to_str(src) + " to " + filename_to_str(target), errno));
      }
    }
  }
  file_helper_.reopen(true);
}

// delete the target if exists, and rename the src file  to target
// return true on success, false otherwise.
template <typename Mutex>
SPDLOG_INLINE bool compressed_rotating_file_sink<Mutex>::rename_file(const filename_t& src_filename, const filename_t& target_filename) {
  // try to delete the target file in case it already exists.
  (void)details::os::remove(target_filename);
  return details::os::rename(src_filename, target_filename) == 0;
}

template <typename Mutex>
SPDLOG_INLINE void compressed_rotating_file_sink<Mutex>::compress_() {
  using details::os::filename_to_str;
  using details::os::path_exists;

  filename_t basename_local;
  path dir_local = dir_;
  const std::string comp_ext = Utility::compressedFileExt;

  if (dir_local.string().empty()) {
    dir_local = "./";
    basename_local = "./" + basename_;
  } else {
    basename_local = dir_local.string() + "/" + basename_;
  }
  std::size_t max_itr_value = (max_compressed_files_ + max_files_ - 1);

  /* Regex for oldest compressed file */
  std::string regex_oldest_file_str = basename_local + "." + std::to_string(max_itr_value) + file_ext_ + "(.*)" + comp_ext;
  std::string regex_file_str = basename_local + "." + "([0-9]+)" + file_ext_ + "(.*)" + comp_ext;
  try {
    if (exists(dir_local)) {
      filename_t src_name;
      filename_t target_file;
      std::smatch match_value;
      filename_t name_replace;
      filename_t name_replace_with;

      for (auto x : directory_iterator(dir_local)) {
        src_name = x.path().string();
        if (std::regex_match(src_name, std::regex(regex_oldest_file_str))) {
          // Delete the oldest compressed file
          details::os::remove(src_name);
        }

        else if (std::regex_match(src_name, match_value, std::regex(regex_file_str))) {
          target_file = src_name;
          name_replace = basename_local + "." + match_value[1].str();
          name_replace_with = basename_local + "." + std::to_string(std::stoi(match_value[1].str()) + 1);
          boost::algorithm::replace_first(target_file, name_replace, name_replace_with);

          if (!rename_file(src_name, target_file)) {
            // if failed try again after a small delay.
            // this is a workaround to a windows issue, where very high rotation
            // rates can cause the rename to fail with permission denied (because of antivirus?).
            details::os::sleep_for_millis(10);
            if (!rename_file(src_name, target_file)) {
              SPDLOG_THROW(spdlog_ex("rotating_file_sink: failed renaming " + filename_to_str(src_name) + " to " + filename_to_str(target_file), errno));
            }
          }
        }  // else if(regex match)
      }    // for(directory_itr)
    }      // directory exists
  } catch (const filesystem_error& err) {
    SPDLOG_THROW(spdlog_ex("Exception in boost filesystem "));
  }

  filename_t file_to_compress = calc_filename(base_filename_, (max_files_));
  if (path_exists(file_to_compress)) {
    filename_t new_compressed_file;
    new_compressed_file = file_to_compress + "." + Utility::getTime();
    if (Utility::compressFile(new_compressed_file, file_to_compress)) {
      details::os::remove(file_to_compress);
    }
  }
}  // compress_()
}  // namespace sinks
}  // namespace spdlog

#endif