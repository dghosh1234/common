Option Explicit

Dim tbl, col
Dim fso, file, filePath
Dim dataTypeRaw, semantics, suggestion, len, lineOut

' File path on desktop
filePath = CreateObject("WScript.Shell").SpecialFolders("Desktop") & "\varchar2_semantics_report.csv"

Set fso = CreateObject("Scripting.FileSystemObject")
Set file = fso.CreateTextFile(filePath, True)

file.WriteLine "Table Name,Column Name,Data Type,Length,Semantics,Suggestion"

For Each tbl In ActiveModel.Tables
    If Not tbl.IsShortcut Then
        For Each col In tbl.Columns
            dataTypeRaw = UCase(col.DataType)
            
            If InStr(dataTypeRaw, "VARCHAR2") > 0 Then
                ' Default values
                len = col.Length
                semantics = ""
                suggestion = ""

                ' Detect semantics from data type
                If InStr(dataTypeRaw, "CHAR") > 0 Then
                    semantics = "CHAR"
                ElseIf InStr(dataTypeRaw, "BYTE") > 0 Then
                    semantics = "BYTE"
                Else
                    semantics = ""  ' Means BYTE is implicit
                End If

                ' If not already CHAR, suggest changing
                If semantics <> "CHAR" Then
                    suggestion = "Change it to VARCHAR2(" & len & " CHAR)"
                    lineOut = """" & tbl.Code & """,""" & col.Code & """,""" & col.DataType & """,""" & len & """,""" & semantics & """,""" & suggestion & """"
                    file.WriteLine lineOut
                End If
            End If
        Next
    End If
Next

file.Close

MsgBox "✅ Report generated successfully at: " & filePath
