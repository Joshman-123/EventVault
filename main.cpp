#include "EventVault.hpp"

int main()
{
    // Example usage:
    evt::EventConfig config{};
    config.m_ringSize = 50;
    evt::EventVault::getInstance().init(config);
    evt::EventVault::getInstance().recordEvent("Application started");

    return 0;
}