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
const CORE_SERVER = process.env.CORE_SERVER ? process.env.CORE_SERVER : env.CORE_SERVER ? env.CORE_SERVER : "http://test-core.chip-in.net";
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
            let result = await dadget.exec(0, {
                type: "insert",
                target: id,
                new: data
            });
            let count = await dadget.count({}, result.csn);
            chai.assert.equal(count, 1);

            result = await dadget.exec(0, {
                type: "update",
                target: id,
                operator: { $set: { name: "b" } }
            });
            count = await dadget.count({ name: "b" }, result.csn);
            chai.assert.equal(count, 1);

            result = await dadget.exec(0, {
                type: "delete",
                target: id,
            });
            count = await dadget.count({}, result.csn);
            chai.assert.equal(count, 0);
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
            let result = await dadget.exec(0, {
                type: "insert",
                target: id,
                new: data
            });
            let count = await dadget.count({}, result.csn);
            chai.assert.equal(count, 1);

            result = await dadget.exec(0, {
                type: "upsert",
                target: id,
                operator: { $set: { name: "b", prop: "pp" } },
                new: data,
            });
            count = await dadget.count({ name: "b" }, result.csn);
            chai.assert.equal(count, 1);

            result = await dadget.exec(0, {
                type: "upsert",
                target: id,
                new: data,
            });
            count = await dadget.count({ name: "a" }, result.csn);
            chai.assert.equal(count, 1);
            count = await dadget.count({ prop: "pp" }, result.csn);
            chai.assert.equal(count, 1);

            let id2 = Dadget.uuidGen();
            let data2 = {
                name: "cc",
                unique: Dadget.uuidGen(),
            };
            result = await dadget.exec(0, {
                type: "upsert",
                target: id2,
                operator: { $set: { name: "c" } },
                new: data2,
            });
            count = await dadget.count({ name: "cc" }, result.csn);
            chai.assert.equal(count, 1);
            count = await dadget.count({}, result.csn);
            chai.assert.equal(count, 2);
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
            let result = await dadget.exec(0, {
                type: "replace",
                target: id,
                new: data
            });
            let count = await dadget.count({}, result.csn);
            chai.assert.equal(count, 1);

            delete data.prop;
            result = await dadget.exec(0, {
                type: "replace",
                target: id,
                new: data,
            });
            count = await dadget.count({ name: "aa" }, result.csn);
            chai.assert.equal(count, 1);
            count = await dadget.count({ prop: "pp" }, result.csn);
            chai.assert.equal(count, 0);
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
            let result = await dadget.exec(0, {
                type: "insert",
                target: id,
                new: data
            }, { upsertOnUniqueError: true });
            let count = await dadget.count({}, result.csn);
            chai.assert.equal(count, 1);

            result = await dadget.exec(0, {
                type: "insert",
                target: Dadget.uuidGen(),
                operator: { $set: { name: "b", prop: "pp" } },
                new: data,
            }, { upsertOnUniqueError: true });
            count = await dadget.count({ name: "b" }, result.csn);
            chai.assert.equal(count, 1);
            count = await dadget.count({}, result.csn);
            chai.assert.equal(count, 1);

            data.name = "c";
            result = await dadget.exec(0, {
                type: "insert",
                target: Dadget.uuidGen(),
                new: data,
            }, { upsertOnUniqueError: true });
            count = await dadget.count({ name: "c" }, result.csn);
            chai.assert.equal(count, 1);
            count = await dadget.count({ prop: "pp" }, result.csn);
            chai.assert.equal(count, 1);
            count = await dadget.count({}, result.csn);
            chai.assert.equal(count, 1);
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
            let result = await dadget.exec(0, {
                type: "insert",
                target: id,
                new: data
            }, { replaceOnUniqueError: true });
            let count = await dadget.count({}, result.csn);
            chai.assert.equal(count, 1);

            delete data.prop;
            result = await dadget.exec(0, {
                type: "insert",
                target: Dadget.uuidGen(),
                new: data,
            }, { replaceOnUniqueError: true });
            count = await dadget.count({ name: "aa" }, result.csn);
            chai.assert.equal(count, 1);
            count = await dadget.count({ prop: "pp" }, result.csn);
            chai.assert.equal(count, 0);
        } catch (e) {
            throw e.toString();
        }
    });
});
