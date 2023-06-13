const expensive = true; //want direct feedback when using floating promises. The downside (increase in memory use) is low, considering these are small ts projects anyway
const errLevel = process.env["ESLINT_STRICT"] ? "error" : "warn";
module.exports = {
  parser: "@typescript-eslint/parser", // Specifies the ESLint parser
  extends: [
    "prettier", // Uses eslint-config-prettier to disable ESLint rules from @typescript-eslint/eslint-plugin that would conflict with prettier
  ],
  parserOptions: {
    ecmaVersion: 2018, // Allows for the parsing of modern ECMAScript features
    sourceType: "module", // Allows for the use of imports,
    project: expensive ? "./tsconfig-build.json" : undefined,
    tsconfigRootDir: expensive ? '.' : undefined,
  },
  plugins: ["@typescript-eslint"],
  rules: {
    ...(expensive ? { "@typescript-eslint/no-floating-promises": errLevel } : {})
  },
  ignorePatterns: ["lib", "*.min.js"],
};
