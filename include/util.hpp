
#pragma once
#include <string>
#include <ctime>
#include <filesystem>

inline std::string basename_of(const std::string& path) {
    return std::filesystem::path(path).filename().string();
}

inline std::string change_extension(const std::string& path, const std::string& new_ext) {
    auto p = std::filesystem::path(path);
    p.replace_extension(new_ext);
    return p.string();
}

inline std::string format_time(std::time_t t, const char* fmt) {
    char buf[64];
    std::tm tm{};
#if defined(_WIN32)
    localtime_s(&tm, &t);
#else
    localtime_r(&t, &tm);
#endif
    std::strftime(buf, sizeof(buf), fmt, &tm);
    return std::string(buf);
}

inline std::string substitute_template(std::string tpl,
                                       const std::string& shortName,
                                       std::time_t startTime,
                                       const std::string& basename) {
    // Replace tokens: {shortName},{yyyy},{MM},{dd},{basename}
    auto replace = [&](const std::string& key, const std::string& val) {
        size_t pos = 0;
        while ((pos = tpl.find(key, pos)) != std::string::npos) {
            tpl.replace(pos, key.size(), val);
            pos += val.size();
        }
    };
    replace("{shortName}", shortName);
    replace("{yyyy}", format_time(startTime, "%Y"));
    replace("{MM}", format_time(startTime, "%m"));
    replace("{dd}", format_time(startTime, "%d"));
    replace("{basename}", basename);
    return tpl;
}
