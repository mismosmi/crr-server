/** @type {import('jest').Config} */
const config = {
  testMatch: ["<rootDir>/tests/**/*.[jt]s"],
  transform: {
    "^.+\\.(t|j)sx?$": "@swc/jest",
  },
};

export default config;
