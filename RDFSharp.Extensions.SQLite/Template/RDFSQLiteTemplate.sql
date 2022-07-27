CREATE TABLE [ContextsRegister] (
	[ContextID] integer COLLATE BINARY NOT NULL PRIMARY KEY, 
	[Context] nvarchar(1000) COLLATE BINARY NOT NULL
)
GO

CREATE TABLE [ResourcesRegister] (
	[ResourceID] integer COLLATE BINARY NOT NULL PRIMARY KEY, 
	[Resource] nvarchar(1000) COLLATE BINARY NOT NULL
)
GO

CREATE TABLE [IndexedQuadruples] (
	[QuadrupleID] integer COLLATE BINARY NOT NULL PRIMARY KEY, 
	[TripleFlavor] integer COLLATE BINARY NOT NULL, 
	[SubjectID] integer COLLATE BINARY NOT NULL, 
	[PredicateID] integer COLLATE BINARY NOT NULL, 
	[ObjectID] integer COLLATE BINARY NOT NULL, 
	[ContextID] integer COLLATE BINARY NOT NULL, 
	FOREIGN KEY ([ContextID])
		REFERENCES [ContextsRegister] ([ContextID])
		ON UPDATE NO ACTION ON DELETE CASCADE, 
	FOREIGN KEY ([SubjectID])
		REFERENCES [ResourcesRegister] ([ResourceID])
		ON UPDATE NO ACTION ON DELETE CASCADE, 
	FOREIGN KEY ([PredicateID])
		REFERENCES [ResourcesRegister] ([ResourceID])
		ON UPDATE NO ACTION ON DELETE CASCADE, 
	FOREIGN KEY ([ObjectID])
		REFERENCES [ResourcesRegister] ([ResourceID])
		ON UPDATE NO ACTION ON DELETE CASCADE
)
GO

CREATE VIEW Quadruples AS
SELECT
	IQ.QuadrupleID AS QuadrupleID,
	IQ.TripleFlavor AS TripleFlavor,
	IQ.ContextID As ContextID,
	CR.Context AS Context,
	IQ.SubjectID AS SubjectID,
	RS.Resource AS Subject,
	IQ.PredicateID AS PredicateID,
	RP.Resource AS Predicate,
	IQ.ObjectID AS ObjectID,
	RO.Resource As Object
FROM
	IndexedQuadruples AS IQ INNER JOIN ContextsRegister AS CR ON IQ.ContextID = CR.ContextID
		INNER JOIN ResourcesRegister AS RS ON IQ.SubjectID = RS.ResourceID
			INNER JOIN ResourcesRegister AS RP ON IQ.PredicateID = RP.ResourceID
				INNER JOIN ResourcesRegister AS RO ON IQ.ObjectID = RO.ResourceID