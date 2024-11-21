#include <iostream>
#include <fstream>
#include <cstdlib>
#include <ctime>
#include <string>

std::string generateRandomCart() {
    char letter = 'a' + rand() % 3; // 'a', 'b' или 'c'
    int number = 1 + rand() % 9; // Число от 1 до 9
    return std::string(1, letter) + std::to_string(number);
}

int main() {
    std::ofstream outputFile("sells.csv");
    if (!outputFile.is_open()) {
        std::cerr << "Ошибка при открытии файла!" << std::endl;
        return 1;
    }

    outputFile << "cart,id\n"; // Записываем заголовки

    srand(static_cast<unsigned int>(time(0))); // Инициализация генератора случайных чисел

    int numEntries = 100000; // Количество записей (вы можете изменить это значение)
    for (int i = 0; i < numEntries; ++i) {
        std::string cartValue = generateRandomCart();
        int idValue = 1 + rand() % 30000; // ID от 1 до 13
        outputFile << cartValue << "," << idValue << "\n"; // Записываем данные
    }

    outputFile.close(); // Закрываем файл
    std::cout << "Файл sells.csv успешно создан!" << std::endl;

    return 0;
}