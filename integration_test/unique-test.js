const chai = require('chai');
const chaiAsPromised = require("chai-as-promised");
const { ResourceNode } = require('@chip-in/resource-node');
const Dadget = require('../lib/se/Dadget').default;
const fs = require('fs');

const expect = chai.expect;
chai.use(chaiAsPromised);

let env = {}
try {
    let text = fs.readFileSync('/proc/1/environ', { encoding: 'utf-8' });
    let envList = text.split("\0");
    envList.forEach(row => {
        let data = row.split("=", 2);
        env[data[0]] = data[1];
    })
} catch (e) { }
const CORE_SERVER = process.env.CORE_SERVER ? process.env.CORE_SERVER : env.CORE_SERVER ? env.CORE_SERVER : "http://core";
let node = new ResourceNode(CORE_SERVER, "test-client");
if (process.env.ACCESS_TOKEN) {
    node.setJWTAuthorization(process.env.ACCESS_TOKEN);
}
Dadget.registerServiceClasses(node);

describe('unique', function () {
    before(async () => {
        await node.start();
    });

    after(async () => {
        await node.stop();
    });

    it('1', async () => {
        let dadget = Dadget.getDb(node, "test1");
        await dadget.clear();
        let data = {
            name: "a",
            unique: "a",
        };
        try {
            await dadget.exec(0, {
                type: "insert",
                target: 1,
                new: data
            });
            await expect(
                dadget.exec(0, {
                    type: "insert",
                    target: 2,
                    new: data
                })
            ).to.be.rejected;

            data.unique = "b";
            await dadget.exec(0, {
                type: "insert",
                target: 2,
                new: data
            });
            await dadget.exec(0, {
                type: "update",
                target: 2,
                operator: { $set: { unique: "b" } }
            })
            await expect(
                dadget.exec(0, {
                    type: "update",
                    target: 2,
                    operator: { $set: { unique: "a" } }
                })
            ).to.be.rejected;
            await dadget.exec(0, {
                type: "delete",
                target: 2
            });
            await dadget.exec(0, {
                type: "insert",
                target: 2,
                new: data
            });
            chai.assert.equal(await dadget.count({}), 2);

            data.unique = null;
            await dadget.exec(0, {
                type: "insert",
                target: 3,
                new: data
            });
            await dadget.exec(0, {
                type: "insert",
                target: 4,
                new: data
            });
        } catch (e) {
            throw e.toString();
        }
    });

    it('2', async () => {
        let dadget = Dadget.getDb(node, "test1");
        await dadget.clear();
        let data = {
            name: "a",
            unique: "a",
        };
        try {
            await dadget.exec(0, {
                type: "insert",
                target: 1,
                new: data
            });
            data.unique = "b";
            await dadget.exec(0, {
                type: "insert",
                target: 2,
                new: data
            });
            await expect(
                dadget.exec(0, {
                    type: "insert",
                    target: 3,
                    new: data,
                    operator: { $set: { unique: "a" } }
                }, { upsertOnUniqueError: true })
            ).to.be.rejected;
            await expect(
                dadget.exec(0, {
                    type: "update",
                    target: 2,
                    operator: { $set: { unique: "a" } }
                })
            ).to.be.rejected;
            await expect(
                dadget.exec(0, {
                    type: "upsert",
                    target: 2,
                    new: data,
                    operator: { $set: { unique: "a" } }
                })
            ).to.be.rejected;
            await expect(
                dadget.exec(0, {
                    type: "upsert",
                    target: 3,
                    new: data,
                    operator: { $set: { unique: "a" } }
                })
            ).to.be.rejected;

        } catch (e) {
            throw e.toString();
        }
    });

    it('3', async () => {
        let dadget = Dadget.getDb(node, "test1");
        await dadget.clear();
        let data = {
            name: "a",
            unique: "a",
        };
        try {
            await expect(Dadget.execTransaction(node, ["test1"], async (test1) => {
                await test1.exec(0, {
                    type: "insert",
                    target: 1,
                    new: data
                });
                throw "test";
            })).to.be.rejectedWith("test");

            await dadget.exec(0, {
                type: "insert",
                target: 1,
                new: data
            });

            await expect(Dadget.execTransaction(node, ["test1"], async (test1) => {
                await test1.exec(0, {
                    type: "update",
                    target: 1,
                    operator: { $set: { unique: "b" } }
                });
                throw "test";
            })).to.be.rejectedWith("test");

            await expect(
                dadget.exec(0, {
                    type: "insert",
                    target: 2,
                    new: data,
                })
            ).to.be.rejected;

            await expect(Dadget.execTransaction(node, ["test1"], async (test1) => {
                await test1.exec(0, {
                    type: "upsert",
                    target: 1,
                    new: data,
                    operator: { $set: { unique: "b" } }
                });
                chai.assert.equal(await test1.count({}), 1);
                throw "test";
            })).to.be.rejectedWith("test");

            await expect(
                dadget.exec(0, {
                    type: "insert",
                    target: 2,
                    new: data,
                })
            ).to.be.rejected;

            data.unique = "b";
            await expect(Dadget.execTransaction(node, ["test1"], async (test1) => {
                await test1.exec(0, {
                    type: "upsert",
                    target: 2,
                    new: data,
                    operator: { $set: { unique: "b" } }
                });
                chai.assert.equal(await test1.count({}), 2);
                throw "test";
            })).to.be.rejectedWith("test");

            await dadget.exec(0, {
                type: "insert",
                target: 2,
                new: data
            });

            await expect(Dadget.execTransaction(node, ["test1"], async (test1) => {
                await test1.exec(0, {
                    type: "delete",
                    target: 1
                });
                chai.assert.equal(await test1.count({}), 1);
                throw "test";
            })).to.be.rejectedWith("test");

            data.unique = "a";
            await expect(
                dadget.exec(0, {
                    type: "insert",
                    target: 3,
                    new: data,
                })
            ).to.be.rejected;

        } catch (e) {
            throw e.toString();
        }
    });
});
