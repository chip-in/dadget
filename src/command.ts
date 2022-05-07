import { ResourceNode } from "@chip-in/resource-node";
import * as commandLineArgs from "command-line-args";
import * as commandLineUsage from "command-line-usage";
import { Maintenance } from "./Maintenance";
import Dadget from "./se/Dadget";

const usage = () => {
  const sections = [
    {
      header: "Synopsis",
      content: "$ dadget <command>　<options>",
    },
    {
      header: "Command List",
      content: [
        { name: "reset", summary: "Delete local MongoDB data." },
        { name: "clear", summary: "Delete all DB data." },
        { name: "export", summary: "Export DB data." },
        { name: "import", summary: "Import DB data." },
        { name: "backup", summary: "Backup DB data." },
        { name: "restore", summary: "Restore DB data." },
      ],
    },
    {
      header: "Option List",
      optionList: [
        {
          name: "server",
          typeLabel: "{underline url}",
          description: "Core server",
        },
        {
          name: "rn",
          typeLabel: "{underline name}",
          description: "Resource Node name",
        },
        {
          name: "name",
          typeLabel: "{underline name}",
          description: "Database name",
        },
        {
          name: "file",
          typeLabel: "{underline filename}",
          description: "Output or input file",
        },
        {
          name: "id",
          typeLabel: "{underline id}",
          description: "Id column name for import command",
        },
        {
          name: "user",
          typeLabel: "{underline username}",
          description: "Username for basic authorization",
        },
        {
          name: "password",
          typeLabel: "{underline password}",
          description: "Password for basic authorization",
        },
        {
          name: "jwtToken",
          typeLabel: "{underline token}",
          description: "Token for JWT authorization",
        },
        {
          name: "jwtRefreshPath",
          typeLabel: "{underline path}",
          description: "Refresh path for JWT authorization",
        },
        {
          name: "force",
          type: Boolean,
          description: "Forced execution",
        },
      ],
    },
  ];
  const usage = commandLineUsage(sections);
  console.info(usage);
  process.exit();
};

const mainDefinitions = [
  { name: "command", defaultOption: true },
];
const mainOptions = commandLineArgs(mainDefinitions, { stopAtFirstUnknown: true });
const argv = mainOptions._unknown || [];

if (mainOptions.command === "reset") {
  const argsDefinitions = [
    { name: "target", defaultOption: true },
    { name: "name" },
  ];
  const options = commandLineArgs(argsDefinitions, { argv });
  const target = options.target || options.name;
  if (!target) {
    usage();
  }
  Maintenance.reset(target);

} else if (mainOptions.command === "clear" || mainOptions.command === "export" || mainOptions.command === "import" ||
  mainOptions.command === "backup" || mainOptions.command === "restore") {
  const argsDefinitions: any[] = [
    { name: "server" },
    { name: "rn" },
    { name: "name" },
    { name: "user" },
    { name: "password" },
    { name: "jwtToken" },
    { name: "jwtRefreshPath" },
  ];
  if (mainOptions.command === "clear") {
    argsDefinitions.push({ name: "force", type: Boolean });
  } else {
    argsDefinitions.push({ name: "file" });
    argsDefinitions.push({ name: "id" });
  }
  const options = commandLineArgs(argsDefinitions, { argv });
  if (!options.server || !options.rn || !options.name) {
    usage();
  }
  if (mainOptions.command !== "clear" && !options.file) {
    usage();
  }
  if (mainOptions.command === "import" && !options.id) {
    usage();
  }
  Dadget.enableDeleteSubset = false;
  const node = new ResourceNode(options.server, options.rn);
  Dadget.registerServiceClasses(node);
  if (options.user) {
    node.setBasicAuthorization(options.user, options.password);
  }
  if (options.jwtToken) {
    node.setJWTAuthorization(options.jwtToken, options.jwtRefreshPath);
  }
  node.start().then(() => {
    const seList = node.searchServiceEngine("Dadget", { database: options.name });
    if (seList.length === 0) {
      return Promise.reject("Dadget is missing from the RN configuration or the database name is incorrect.");
    }
    const dadget = seList[0] as Dadget;
    if (mainOptions.command === "export") {
      return Maintenance.export(dadget, options.file);
    } else if (mainOptions.command === "import") {
      return Maintenance.import(dadget, options.file, options.id);
    } else if (mainOptions.command === "backup") {
      return Maintenance.export(dadget, options.file);
    } else if (mainOptions.command === "restore") {
      return Maintenance.restore(dadget, options.file);
    } else if (mainOptions.command === "clear") {
      return Maintenance.clear(dadget, !!options.force);
    }
  })
    .then(() => node.stop().then(() => process.exit()))
    .catch((msg) => {
      console.error(msg.toString ? msg.toString() : msg);
      node.stop()
        .then(() => process.exit());
    });
} else {
  usage();
}
