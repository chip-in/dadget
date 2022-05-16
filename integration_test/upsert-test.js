const chai = require('chai');
const { ResourceNode } = require('@chip-in/resource-node');
const Dadget = require('../lib/se/Dadget').default;
const fs = require('fs');

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

describe('dadget', function () {
    before(async () => {
        await node.start();
    });

    after(async () => {
        await node.stop();
    });

    it('基本操作', async () => {
        let dadget = Dadget.getDb(node, "test1");
        await dadget.clear();
        let id = Dadget.uuidGen();
        let unique = Dadget.uuidGen();
        let data = {
            name: "a",
            unique,
        };
        try {
            await dadget.exec(0, {
                type: "insert",
                target: id,
                new: data
            });
            chai.assert.equal(await dadget.count({}), 1);

            await dadget.exec(0, {
                type: "update",
                target: id,
                operator: { $set: { name: "b" } }
            });
            chai.assert.equal(await dadget.count({ name: "b" }), 1);

            await dadget.exec(0, {
                type: "delete",
                target: id,
            });
            chai.assert.equal(await dadget.count({}), 0);
        } catch (e) {
            throw e.toString();
        }
    });

    it('upsert', async () => {
        let dadget = Dadget.getDb(node, "test1");
        await dadget.clear();
        let id = Dadget.uuidGen();
        let unique = Dadget.uuidGen();
        let data = {
            name: "a",
            unique,
        };
        try {
            await dadget.exec(0, {
                type: "insert",
                target: id,
                new: data
            });
            chai.assert.equal(await dadget.count({}), 1);

            await dadget.exec(0, {
                type: "upsert",
                target: id,
                operator: { $set: { name: "b", prop: "pp" } },
                new: data,
            });
            chai.assert.equal(await dadget.count({ name: "b" }), 1);

            await dadget.exec(0, {
                type: "upsert",
                target: id,
                new: data,
            });
            chai.assert.equal(await dadget.count({ name: "a" }), 1);
            chai.assert.equal(await dadget.count({ prop: "pp" }), 1);

            let id2 = Dadget.uuidGen();
            let data2 = {
                name: "cc",
                unique: Dadget.uuidGen(),
            };
            await dadget.exec(0, {
                type: "upsert",
                target: id2,
                operator: { $set: { name: "c" } },
                new: data2,
            });
            chai.assert.equal(await dadget.count({ name: "cc" }), 1);
            chai.assert.equal(await dadget.count({}), 2);

            const before = await dadget.query({ _id: id2 });
            await dadget.exec(0, {
                type: "update",
                target: id2,
                operator: { $set: { name: "d" } },
                before: before.resultSet[0]
            });
            chai.assert.equal(await dadget.count({ name: "d" }), 1);
        } catch (e) {
            throw e.toString();
        }
    });

    it('replace', async () => {
        let dadget = Dadget.getDb(node, "test1");
        await dadget.clear();
        let id = Dadget.uuidGen();
        let unique = Dadget.uuidGen();
        let data = {
            name: "aa",
            prop: "pp",
            unique,
        };
        try {
            await dadget.exec(0, {
                type: "replace",
                target: id,
                new: data
            });
            chai.assert.equal(await dadget.count({}), 1);

            delete data.prop;
            await dadget.exec(0, {
                type: "replace",
                target: id,
                new: data,
            });
            chai.assert.equal(await dadget.count({ name: "aa" }), 1);
            chai.assert.equal(await dadget.count({ prop: "pp" }), 0);
        } catch (e) {
            throw e.toString();
        }
    });

    it('upsertOnUniqueError', async () => {
        let dadget = Dadget.getDb(node, "test1");
        await dadget.clear();
        let id = Dadget.uuidGen();
        let unique = Dadget.uuidGen();
        let data = {
            name: "a",
            unique,
        };
        try {
            await dadget.exec(0, {
                type: "insert",
                target: id,
                new: data
            }, { upsertOnUniqueError: true });
            chai.assert.equal(await dadget.count({}), 1);

            await dadget.exec(0, {
                type: "insert",
                target: Dadget.uuidGen(),
                operator: { $set: { name: "b", prop: "pp" } },
                new: data,
            }, { upsertOnUniqueError: true });
            chai.assert.equal(await dadget.count({ name: "b" }), 1);
            chai.assert.equal(await dadget.count({}), 1);

            data.name = "c";
            await dadget.exec(0, {
                type: "insert",
                target: Dadget.uuidGen(),
                new: data,
            }, { upsertOnUniqueError: true });
            chai.assert.equal(await dadget.count({ name: "c" }), 1);
            chai.assert.equal(await dadget.count({ prop: "pp" }), 1);
            chai.assert.equal(await dadget.count({}), 1);
        } catch (e) {
            throw e.toString();
        }
    });

    it('replaceOnUniqueError', async () => {
        let dadget = Dadget.getDb(node, "test1");
        await dadget.clear();
        let id = Dadget.uuidGen();
        let unique = Dadget.uuidGen();
        let data = {
            name: "aa",
            prop: "pp",
            unique,
        };
        try {
            await dadget.exec(0, {
                type: "insert",
                target: id,
                new: data
            }, { replaceOnUniqueError: true });
            chai.assert.equal(await dadget.count({}), 1);

            delete data.prop;
            await dadget.exec(0, {
                type: "insert",
                target: Dadget.uuidGen(),
                new: data,
            }, { replaceOnUniqueError: true });
            chai.assert.equal(await dadget.count({ name: "aa" }), 1);
            chai.assert.equal(await dadget.count({ prop: "pp" }), 0);
        } catch (e) {
            throw e.toString();
        }
    });

    it('transaction', async () => {
        let dadget = Dadget.getDb(node, "test1");
        await dadget.clear();
        let id = Dadget.uuidGen();
        let unique = Dadget.uuidGen();
        let data = {
            name: "a",
            unique,
        };
        try {
            await Dadget.execTransaction(node, ["test1"], async (test1) => {
                await test1.exec(0, {
                    type: "insert",
                    target: id,
                    new: data
                });
                chai.assert.equal(await test1.count({}), 1);
                throw "test";
            });
        } catch (e) {
        }
        chai.assert.equal(await dadget.count({}), 0);
    });

    it('transaction update', async () => {
        let dadget = Dadget.getDb(node, "test1");
        await dadget.clear();
        let id = Dadget.uuidGen();
        let unique = Dadget.uuidGen();
        let data = {
            name: "a",
            unique,
        };
        await dadget.exec(0, {
            type: "insert",
            target: id,
            new: data
        });
        try {
            await Dadget.execTransaction(node, ["test1"], async (test1) => {
                await test1.exec(0, {
                    type: "update",
                    target: id,
                    operator: {
                        "$set": {
                            "name": "b"
                        }
                    }
                });
                chai.assert.equal(await test1.count({ "name": "b" }), 1);
                chai.assert.equal(await dadget.count({ "name": "b" }), 0);

                let data2 = {
                    name: "cc",
                    unique: Dadget.uuidGen(),
                };
                await test1.exec(0, {
                    type: "upsert",
                    target: id,
                    operator: { $set: { name: "c" } },
                    new: data2,
                });
                chai.assert.equal(await test1.count({ name: "c" }), 1);

                const before = await test1.query({ _id: id });
                await test1.exec(before.csn, {
                    type: "update",
                    target: id,
                    operator: { $set: { name: "d" } },
                });
                chai.assert.equal(await test1.count({ name: "d" }), 1);

                let id2 = Dadget.uuidGen();
                await test1.exec(0, {
                    type: "replace",
                    target: id2,
                    new: data2,
                });
                chai.assert.equal(await test1.count({ name: "cc" }), 1);

                await test1.exec(0, {
                    type: "delete",
                    target: id
                });
                chai.assert.equal(await test1.count({ "name": "d" }), 0);
                throw "test";
            });
        } catch (e) {
            if (e != "test") throw e;
        }
        chai.assert.equal(await dadget.count({ "name": "a" }), 1);
        chai.assert.equal(await dadget.count({ "name": "d" }), 0);
        chai.assert.equal(await dadget.count({ "name": "cc" }), 0);
    });
});
