#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <queue>
#include <thread>
#include <shared_mutex>
#include <condition_variable>
#include <algorithm>
#include <sstream>
#include <atomic>
#include <memory>
#include <windows.h>
#include <commdlg.h>
#include <chrono>
#include <iomanip>
#include <filesystem>
#include <execution> // Для параллельной сортировки
#include <future>

const size_t CHUNK_SIZE = 10 * 1024 * 1024; // Размер блока для чтения файла (10 MB)
std::atomic<bool> reading_done(false); // Атомарная переменная для отслеживания завершения чтения

// Общий мьютекс и условная переменная для управления очередью
std::shared_mutex queue_mutex;
std::condition_variable_any data_condition;
std::deque<std::string> data_queue; // Используем std::deque для очереди

// Мьютекс для записи временных файлов
std::mutex file_mutex;
std::vector<std::string> temp_files;

/* Функция для открытия диалогового окна выбора файла */
std::wstring open_file_selection_dialog() {
    wchar_t filename[MAX_PATH] = { 0 };

    OPENFILENAMEW ofn;
    ZeroMemory(&ofn, sizeof(ofn));
    ofn.lStructSize = sizeof(OPENFILENAMEW);
    ofn.hwndOwner = nullptr; // Если есть окно, передайте его дескриптор
    ofn.lpstrFilter = L"Text Files\0*.TXT\0All Files\0*.*\0";
    ofn.lpstrFile = filename;
    ofn.nMaxFile = MAX_PATH;
    ofn.Flags = OFN_PATHMUSTEXIST | OFN_FILEMUSTEXIST;

    if (GetOpenFileNameW(&ofn)) {
        return std::wstring(filename);
    }
    else {
        std::wcerr << L"No file selected or error occurred." << std::endl;
        return L"";
    }
}

/* Функция для чтения файла */
void read_file_in_chunks(const std::string& filename) {
    std::ifstream infile(filename, std::ios::binary);
    if (!infile.is_open()) {
        std::cerr << "Error opening file: " << filename << std::endl;
        return;
    }

    std::unique_ptr<char[]> buffer(new char[CHUNK_SIZE]);
    std::string chunk;
    chunk.reserve(CHUNK_SIZE);

    while (infile) {
        infile.read(buffer.get(), CHUNK_SIZE);
        size_t bytes_read = infile.gcount();
        chunk.append(buffer.get(), bytes_read);

        size_t last_delim = chunk.find_last_of(" ,");
        if (last_delim != std::string::npos && bytes_read == CHUNK_SIZE) {
            std::string to_queue = chunk.substr(0, last_delim + 1);
            chunk = chunk.substr(last_delim + 1);

            {
                std::unique_lock<std::shared_mutex> lock(queue_mutex);
                data_queue.push_back(std::move(to_queue));
            }
            data_condition.notify_one();
        }
    }

    if (!chunk.empty()) {
        std::unique_lock<std::shared_mutex> lock(queue_mutex);
        data_queue.push_back(std::move(chunk));
    }
    reading_done = true;
    data_condition.notify_all();
}

/* Рабочий поток для обработки данных */
void process_data_chunk(int thread_id) {
    while (true) {
        std::string data_chunk;
        {
            std::unique_lock<std::shared_mutex> lock(queue_mutex);
            data_condition.wait(lock, [] { return !data_queue.empty() || reading_done; });

            if (data_queue.empty() && reading_done) break;
            data_chunk = std::move(data_queue.front());
            data_queue.pop_front();
        }

        std::vector<double> numbers;
        std::istringstream stream(data_chunk);
        std::string token;
        while (std::getline(stream, token, ' ')) {
            try {
                numbers.push_back(std::stod(token));
            }
            catch (...) {}
        }

        auto sort_start_time = std::chrono::high_resolution_clock::now();
        std::sort(std::execution::par, numbers.begin(), numbers.end()); // Параллельная сортировка
        auto sort_end_time = std::chrono::high_resolution_clock::now();
        auto sort_duration = std::chrono::duration_cast<std::chrono::milliseconds>(sort_end_time - sort_start_time);
        std::cout << "Thread " << thread_id << " sorting time: " << sort_duration.count() << " ms" << std::endl;

        std::string temp_filename = "temp_" + std::to_string(thread_id) + "_" + std::to_string(rand()) + ".txt";
        {
            std::lock_guard<std::mutex> lock(file_mutex);
            temp_files.push_back(temp_filename);
            std::cout << "Temporary file created: " << temp_filename << std::endl;
        }

        std::ofstream temp_file(temp_filename, std::ios::binary);
        for (double num : numbers) {
            temp_file << num << " ";
        }
    }
}

/* Функция для объединения временных файлов */
void merge_temporary_files(const std::string& output_file) {
    std::priority_queue<std::pair<double, std::ifstream*>,
        std::vector<std::pair<double, std::ifstream*>>,
        std::greater<>> min_heap;

    std::vector<std::unique_ptr<std::ifstream>> streams;
    for (const std::string& temp_file : temp_files) {
        auto stream = std::make_unique<std::ifstream>(temp_file, std::ios::binary);
        if (!stream->is_open()) {
            std::cerr << "Error opening temp file: " << temp_file << std::endl;
            continue;
        }
        double value;
        if (*stream >> value) {
            min_heap.emplace(value, stream.get());
        }
        streams.push_back(std::move(stream));
    }

    std::ofstream outfile(output_file, std::ios::binary);
    if (!outfile.is_open()) {
        std::cerr << "Error opening output file: " << output_file << std::endl;
        return;
    }

    auto merge_start_time = std::chrono::high_resolution_clock::now();
    while (!min_heap.empty()) {
        auto [value, stream] = min_heap.top();
        min_heap.pop();
        outfile << value << " ";
        if (*stream >> value) {
            min_heap.emplace(value, stream);
        }
    }
    auto merge_end_time = std::chrono::high_resolution_clock::now();
    auto merge_duration = std::chrono::duration_cast<std::chrono::milliseconds>(merge_end_time - merge_start_time);
    std::cout << "Merging time: " << merge_duration.count() << " ms" << std::endl;

    for (const auto& stream : streams) {
        if (stream && stream->is_open()) {
            stream->close();
        }
    }

    for (const auto& temp_file : temp_files) {
        std::cout << "Removing temporary file: " << temp_file << std::endl;
        std::filesystem::remove(temp_file);
    }
}

int main() {
    // Запись времени начала выполнения
    auto start_time = std::chrono::high_resolution_clock::now();

    // Открытие диалогового окна для выбора файла
    std::wstring input_file_w = open_file_selection_dialog();
    if (input_file_w.empty()) {
        std::wcerr << L"No file selected. Exiting." << std::endl;
        return 1;
    }

    // Преобразование в std::string для дальнейшей обработки
    std::string input_file(input_file_w.begin(), input_file_w.end());
    const std::string output_file = "output.txt";

    // Поток для чтения файла
    std::thread reader_thread(read_file_in_chunks, input_file);

    // Рабочие потоки
    const int num_threads = std::thread::hardware_concurrency();
    std::vector<std::thread> worker_threads;
    for (int i = 0; i < num_threads; ++i) {
        worker_threads.emplace_back(process_data_chunk, i);
    }

    // Ожидание завершения потоков
    reader_thread.join();
    for (auto& thread : worker_threads) {
        thread.join();
    }

    // Объединение временных файлов
    merge_temporary_files(output_file);

    // Запись времени окончания выполнения
    auto end_time = std::chrono::high_resolution_clock::now();
    auto total_duration = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time);

    std::cout << "Sorting complete. Output written to " << output_file << std::endl;
    std::cout << "Total execution time: " << total_duration.count() << " seconds" << std::endl;

    return 0;
}
