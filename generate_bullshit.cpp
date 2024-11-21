#include <iostream>
#include <fstream>
#include <cstdlib>
#include <ctime>
#include <string>

std::string generateRandomCart() {
    char letter = 'a' + rand() % 3;
    int number = 1 + rand() % 9;
    return std::string(1, letter) + std::to_string(number);
}

int main() {
    std::ofstream outputFile("sells.csv");
    if (!outputFile.is_open()) {
        std::cerr << "Ошибка при открытии файла!" << std::endl;
        return 1;
    }

    outputFile << "cart,id\n";

    srand(static_cast<unsigned int>(time(0)));

    int numEntries = 100000;
    for (int i = 0; i < numEntries; ++i) {
        std::string cartValue = generateRandomCart();
        int idValue = 1 + rand() % 30000;
        outputFile << cartValue << "," << idValue << "\n";
    }

    outputFile.close();
    std::cout << "Файл sells.csv успешно создан!" << std::endl;

    return 0;
}