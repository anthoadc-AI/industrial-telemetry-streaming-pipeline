'use strict';

// Jest configuration for the Node.js unit tests (Phase 2, Task 2.4).
// Coverage: native V8. Lines are enforced at 80 % (owner decision); branches
// are reported, not enforced.
module.exports = {
  testEnvironment: 'node',
  testMatch: ['<rootDir>/tests/unit/**/*.test.js'],
  coverageProvider: 'v8',
  collectCoverageFrom: ['processing/alarm_consumer/**/*.js', 'common/**/*.js'],
  coverageReporters: ['text', 'text-summary'],
  coverageThreshold: { global: { lines: 80 } },
};
